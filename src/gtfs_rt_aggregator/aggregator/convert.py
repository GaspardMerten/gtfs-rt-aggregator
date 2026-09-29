"""Rewrite realtime files stored before 0.6.0 with the current types."""

import logging
import os
import tempfile
from datetime import datetime
from typing import Dict

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..config.models import GtfsRtConfig
from ..schema.conform import conform
from ..storage.base import StorageInterface

logger = logging.getLogger(__name__)


def aggregated_root(config: GtfsRtConfig, provider: str, service: str) -> str:
    """Folder holding every aggregated file of a provider's service."""
    template = config.output.path_template
    prefix = template[: template.index("{start")]
    return prefix.format(provider=provider, service=service).rsplit("/", 1)[0]


def convert_old_files(
    config: GtfsRtConfig, storages: Dict[str, StorageInterface]
) -> int:
    """
    Rewrite the aggregated realtime files that still have the types of
    earlier versions (fetchTime as Unix seconds, unsigned integers), so query
    engines can read old and new files as one table. Returns how many files
    were rewritten. Each file is read whole: run it once, when upgrading.
    """
    converted = 0
    with tempfile.TemporaryDirectory(prefix="gtfs_rt_aggregator-convert-") as tmp:
        local = os.path.join(tmp, "file.parquet")
        for provider in config.providers:
            storage = storages.get(provider.name, storages["global"])
            tz = pytz.timezone(provider.timezone)
            services = sorted({s for api in provider.realtime for s in api.services})
            for service in services:
                root = aggregated_root(config, provider.name, service)
                for path in storage.walk_files(root):
                    if not path.endswith(".parquet"):
                        continue
                    storage.read_to_file(path, local)
                    fetch_time = pq.read_schema(local).field("fetchTime").type
                    if not pa.types.is_integer(fetch_time):
                        continue
                    table = conform(pq.read_table(local), service, provider.name, tz)
                    pq.write_table(table, local, compression="brotli")
                    storage.save_file(local, path)
                    converted += 1
                    logger.info(f"Converted {path} ({table.num_rows} rows)")
    return converted
