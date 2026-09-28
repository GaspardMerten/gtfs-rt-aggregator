from datetime import datetime, timezone
from typing import Optional

# Individual files and static versions are named after their fetch time in UTC,
# e.g. 2026-10-25_01-30-00Z. UTC never repeats an hour, and the name has no "+"
# (which some tools read as a space in object keys).
FILE_TIME_FORMAT = "%Y-%m-%d_%H-%M-%SZ"

# Older names: local time with UTC offset (0.3.0), local time only (before)
_OFFSET_FORMAT = "%Y-%m-%d_%H-%M-%S%z"
_LOCAL_FORMAT = "%Y-%m-%d_%H-%M-%S"


def format_file_time(dt: datetime) -> str:
    """Name for an aware datetime."""
    return dt.astimezone(timezone.utc).strftime(FILE_TIME_FORMAT)


def parse_file_time(name: str) -> Optional[datetime]:
    """
    Parse a name written by format_file_time, or an older one.

    Returns an aware datetime, a naive local datetime for names from before
    0.3.0, or None if the name does not match.
    """
    try:
        return datetime.strptime(name, FILE_TIME_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        pass
    for fmt in (_OFFSET_FORMAT, _LOCAL_FORMAT):
        try:
            return datetime.strptime(name, fmt)
        except ValueError:
            pass
    return None
