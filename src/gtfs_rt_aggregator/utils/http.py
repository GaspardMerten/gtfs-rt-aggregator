import hashlib
import logging
import time
from typing import Callable, Dict, Optional, Tuple, TypeVar

import requests

T = TypeVar("T")

# Worth retrying: the server may answer the next request
RETRY_STATUSES = {429, 500, 502, 503, 504}


class RetryableStatus(requests.HTTPError):
    """HTTP status that is worth retrying."""


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
) -> Tuple[int, str]:
    """
    Stream url to path, with retries. Returns its size and sha256.

    The body is written as it arrives, so a large feed never sits in memory.
    """

    def attempt() -> Tuple[int, str]:
        digest = hashlib.sha256()
        size = 0
        with requests.get(
            url, headers=headers, timeout=timeout, stream=True
        ) as response:
            raise_for_status(response)
            with open(path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
        return size, digest.hexdigest()

    return with_retries(attempt, retries, logger, f"GET {url}")


def raise_for_status(response: requests.Response):
    """Like response.raise_for_status(), but marks statuses worth retrying."""
    if response.status_code in RETRY_STATUSES:
        raise RetryableStatus(
            f"{response.status_code} {response.reason} for {response.url}",
            response=response,
        )
    response.raise_for_status()


def get_bytes(
    url: str,
    headers: Optional[Dict[str, str]],
    retries: int,
    logger: logging.Logger,
    timeout: float = 60,
) -> bytes:
    """GET url and return the body, with retries."""

    def attempt() -> bytes:
        response = requests.get(url, headers=headers, timeout=timeout)
        raise_for_status(response)
        return response.content

    return with_retries(attempt, retries, logger, f"GET {url}")
