"""Remove temporary files left behind by jobs that were killed."""

import logging
import os
import shutil
import socket
import tempfile
import time

logger = logging.getLogger(__name__)

# Older than any job can run: a static conversion takes minutes
STALE_AFTER_SECONDS = 12 * 3600
# Filter caches are rebuilt when missing
CACHE_STALE_AFTER_SECONDS = 7 * 24 * 3600

STATIC_WORK_PREFIX = "gtfs_rt_aggregator-static-"


def clean_stale_temp_files(temp_dir: str = None, now: float = None) -> int:
    """
    Delete this package's stale temporary folders and return how many were
    removed:

    - static work folders (gtfs_rt_aggregator-static-*, and before 0.5.1
      tmp* folders holding only feed.zip and/or parquet/)
    - old filter cache files (gtfs_rt_aggregator-<user>/filter-*.json)
    - multiprocessing Manager folders (pymp-*) whose socket nobody listens to

    Only folders owned by the current user and older than STALE_AFTER_SECONDS.
    """
    temp_dir = temp_dir or tempfile.gettempdir()
    now = now or time.time()
    removed = 0
    try:
        entries = list(os.scandir(temp_dir))
    except OSError:
        return 0

    for entry in entries:
        try:
            if not entry.is_dir(follow_symlinks=False) or not _owned(entry):
                continue
            age = now - entry.stat(follow_symlinks=False).st_mtime
            if entry.name.startswith(
                "gtfs_rt_aggregator-"
            ) and not entry.name.startswith(STATIC_WORK_PREFIX):
                removed += _clean_cache(entry.path, now)
                continue
            if age < STALE_AFTER_SECONDS:
                continue
            if (
                entry.name.startswith(STATIC_WORK_PREFIX)
                or (
                    entry.name.startswith("tmp") and _is_old_static_work_dir(entry.path)
                )
                or (entry.name.startswith("pymp-") and not _has_live_socket(entry.path))
            ):
                shutil.rmtree(entry.path, ignore_errors=True)
                logger.info(f"Removed stale temporary folder {entry.path}")
                removed += 1
        except OSError:
            continue
    return removed


def _owned(entry: os.DirEntry) -> bool:
    try:
        return entry.stat(follow_symlinks=False).st_uid == os.getuid()
    except AttributeError:  # Windows
        return True


def _is_old_static_work_dir(path: str) -> bool:
    names = set(os.listdir(path))
    return bool(names) and names <= {"feed.zip", "parquet"}


def _has_live_socket(path: str) -> bool:
    """True if a Manager still listens on a socket in this folder."""
    for name in os.listdir(path):
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            client.connect(os.path.join(path, name))
            return True
        except OSError:
            continue
        finally:
            client.close()
    return False


def _clean_cache(path: str, now: float) -> int:
    removed = 0
    for name in os.listdir(path):
        file = os.path.join(path, name)
        if (
            name.startswith("filter-")
            and now - os.path.getmtime(file) > CACHE_STALE_AFTER_SECONDS
        ):
            os.remove(file)
            removed += 1
    return removed
