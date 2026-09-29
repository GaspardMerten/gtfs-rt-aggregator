"""Where aggregated files are stored (see output.path_template)."""

import posixpath
from datetime import date, datetime
from typing import List

from ..config.models import GtfsRtConfig


def aggregated_root(config: GtfsRtConfig, provider: str, service: str) -> str:
    """Folder holding every aggregated file of a provider's service."""
    template = config.output.path_template
    prefix = template[: template.index("{start")]
    return prefix.format(provider=provider, service=service).rsplit("/", 1)[0]


def day_folder(config: GtfsRtConfig, provider: str, service: str, day: date, tz) -> str:
    """Folder of a local day's files (path_template has one folder per day)."""
    start = tz.localize(datetime(day.year, day.month, day.day))
    return posixpath.dirname(
        config.output.path_template.format(
            provider=provider, service=service, start=start, end=start
        )
    )


def day_files(storage, folder: str) -> List[str]:
    """Parquet files of a day folder, sorted."""
    return sorted(
        f
        for f in storage.list_files(folder, "*.parquet")
        # Some backends also list files in subfolders
        if posixpath.dirname(f) == folder
    )
