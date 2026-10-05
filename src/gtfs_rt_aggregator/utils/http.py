import hashlib
import logging
import threading
import time
from typing import Callable, Dict, Optional, Tuple, TypeVar

import requests

from .redact import redact, strip_query

T = TypeVar("T")

# Worth retrying: the server may answer the next request
RETRY_STATUSES = {429, 500, 502, 503, 504}


class RetryableStatus(requests.HTTPError):
    """HTTP status that is worth retrying."""


class DownloadTooLong(IOError):
    """A download went on for longer than allowed (a server trickling bytes)."""


class IncompleteDownload(IOError):
    """A body shorter or longer than its Content-Length (a connection cut): worth retrying."""


def _user_agent() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        name = f"gtfs-rt-aggregator/{version('gtfs_rt_aggregator')}"
    except PackageNotFoundError:  # run from a source checkout
        name = "gtfs-rt-aggregator"
    return f"{name} (+https://github.com/GaspardMerten/gtfs-rt-aggregator)"


# Sent unless the configuration sets its own: some firewalls block requests'
# default (python-requests/x.y)
USER_AGENT = _user_agent()


def request_headers(headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    """headers with the default User-Agent added; a User-Agent in headers wins."""
    result = dict(headers or {})
    if not any(name.lower() == "user-agent" for name in result):
        result["User-Agent"] = USER_AGENT
    return result


def check_length(response: requests.Response, size: int):
    """
    Raise IncompleteDownload if size (bytes received) differs from the
    Content-Length. Not checked when the body was compressed in transit
    (Content-Encoding): the length is then the compressed one.
    """
    expected = response.headers.get("Content-Length")
    if not expected or response.headers.get("Content-Encoding", "identity") != "identity":
        return
    try:
        expected = int(expected)
    except ValueError:
        return
    if size != expected:
        raise IncompleteDownload(
            f"GET {strip_query(response.url or '')} sent {size} of {expected} bytes"
        )


def with_retries(
    action: Callable[[], T],
    retries: int,
    logger: logging.Logger,
    what: str,
    backoff_seconds: float = 1.0,
) -> T:
    """
    Run action, retrying on connection errors (including TLS drops and
    connections cut while reading the body), timeouts and RETRY_STATUSES.

    Waits backoff_seconds, then twice as long after each failure. Other errors,
    such as a 404, are raised right away.
    """
    for attempt in range(retries + 1):
        try:
            return action()
        except (
            requests.ConnectionError,
            requests.Timeout,
            requests.exceptions.ChunkedEncodingError,
            RetryableStatus,
            IncompleteDownload,
        ) as e:
            if attempt == retries:
                raise
            delay = backoff_seconds * 2**attempt
            # A server that answers 429 may say how long to wait
            retry_after = _retry_after(e)
            if retry_after is not None:
                delay = min(max(delay, retry_after), MAX_RETRY_AFTER_SECONDS)
            logger.warning(
                f"{what} failed ({e.__class__.__name__}: {e}), retry {attempt + 1}/{retries} in {delay:.0f}s"
            )
            time.sleep(delay)
    raise AssertionError("unreachable")


MAX_RETRY_AFTER_SECONDS = 120


def _retry_after(error: Exception) -> Optional[float]:
    response = getattr(error, "response", None)
    if response is None or getattr(response, "status_code", None) != 429:
        return None
    try:
        return float(response.headers.get("Retry-After"))
    except (TypeError, ValueError):
        return None


def download_to(
    url: str,
    headers: Optional[Dict[str, str]],
    path: str,
    retries: int,
    logger: logging.Logger,
    timeout: float = 60,
    max_seconds: float = 3600,
) -> Tuple[int, str]:
    """
    Stream url to path, with retries. Returns its size and sha256.

    The body is written as it arrives, so a large feed never sits in memory.
    timeout applies to each read; max_seconds to a whole attempt (a server
    sending a byte every minute never times out otherwise). A body cut short
    (shorter than its Content-Length) is retried.
    """
    headers = request_headers(headers)

    def attempt() -> Tuple[int, str]:
        digest = hashlib.sha256()
        size = 0
        with requests.get(
            url, headers=headers, timeout=timeout, stream=True
        ) as response:
            raise_for_status(response)
            # Reads block until data arrives: a timer closes the connection
            # when the attempt takes too long, which ends the read
            state = {"reading": True, "timed_out": False}

            def expire():
                if state["reading"]:
                    state["timed_out"] = True
                    _abort(response)

            timer = threading.Timer(max_seconds, expire)
            timer.daemon = True
            timer.start()
            try:
                with open(path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                        digest.update(chunk)
                        size += len(chunk)
                state["reading"] = False
            except Exception:
                if state["timed_out"]:
                    raise DownloadTooLong(f"GET {url} took over {max_seconds:.0f}s")
                raise
            finally:
                state["reading"] = False
                timer.cancel()
            check_length(response, size)
        return size, digest.hexdigest()

    return with_retries(attempt, retries, logger, f"GET {url}")


def _abort(response: requests.Response):
    """
    Stop a read blocked in another thread: closing the response is not
    enough, its socket must be shut down (then the read fails at once).
    """
    import socket

    candidates = []
    # The socket under http.client's response (urllib3 detaches it from the
    # connection once the response started)
    raw = getattr(getattr(getattr(response.raw, "_fp", None), "fp", None), "raw", None)
    candidates.append(getattr(raw, "_sock", None))
    connection = getattr(response.raw, "connection", None)
    candidates.append(getattr(connection, "sock", None))
    for sock in candidates:
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
    response.close()


def raise_for_status(response: requests.Response):
    """
    Like response.raise_for_status(), but marks statuses worth retrying, and
    the message holds the start of the body (what the server says went
    wrong) and the URL without its query string (which may hold a key).
    """
    if response.status_code < 400:
        return
    message = f"{response.status_code} {response.reason} for {strip_query(response.url or '')}"
    excerpt = body_excerpt(response)
    if excerpt:
        message += f": {excerpt}"
    if response.status_code in RETRY_STATUSES:
        raise RetryableStatus(message, response=response)
    raise requests.HTTPError(message, response=response)


def body_excerpt(response: requests.Response, limit: int = 200) -> str:
    """The first limit characters of a response's body, on one line, with URL query strings hidden."""
    try:
        if getattr(response, "_content_consumed", False):  # not streamed: already read
            data = (response.content or b"")[: limit * 4]
        else:
            data = response.raw.read(limit * 4, decode_content=True) or b""
    except Exception:
        return ""
    text = " ".join(data.decode("utf-8", "replace").split())
    # Keep printable characters only (a binary body)
    text = "".join(c if c.isprintable() else "?" for c in text)
    return redact(text)[:limit]


def get_bytes(
    url: str,
    headers: Optional[Dict[str, str]],
    retries: int,
    logger: logging.Logger,
    timeout: float = 60,
) -> bytes:
    """GET url and return the body, with retries."""
    headers = request_headers(headers)

    def attempt() -> bytes:
        response = requests.get(url, headers=headers, timeout=timeout)
        raise_for_status(response)
        check_length(response, len(response.content))
        return response.content

    return with_retries(attempt, retries, logger, f"GET {url}")
