"""Keep secrets (API keys in URLs and headers) out of logs and status files."""

import logging
import re

# Query string of a URL: API keys are often passed there (?key=...)
_URL_QUERY = re.compile(r"(\b[a-z][a-z0-9+.-]*://[^\s?#'\"<>]+)\?[^\s#'\"<>]*", re.I)
# A headers dict as printed by repr(), e.g. in job descriptions
_HEADERS = re.compile(r"('headers':\s*)\{[^{}]*\}")


def redact(text: str) -> str:
    """Hide URL query strings and header values in text."""
    return _HEADERS.sub(r"\1{***}", _URL_QUERY.sub(r"\1?***", text))


class RedactingFilter(logging.Filter):
    """Logging filter applying redact() to messages and tracebacks."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = None
        if record.exc_info:
            record.exc_text = redact(
                logging.Formatter().formatException(record.exc_info)
            )
            # exc_text is printed instead
            record.exc_info = None
        if record.stack_info:
            record.stack_info = redact(record.stack_info)
        return True


def install_redaction(logger: logging.Logger = None):
    """Add RedactingFilter to the handlers of logger (default: root), once."""
    for handler in (logger or logging.getLogger()).handlers:
        if not any(isinstance(f, RedactingFilter) for f in handler.filters):
            handler.addFilter(RedactingFilter())
