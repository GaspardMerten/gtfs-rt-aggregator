import logging
import time
from typing import Callable, Dict, Optional, TypeVar

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
            logger.warning(
                f"{what} failed ({e.__class__.__name__}: {e}), retry {attempt + 1}/{retries} in {delay:.0f}s"
            )
            time.sleep(delay)
    raise AssertionError("unreachable")


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
