"""Rewrite realtime files stored before 0.6.0 with the current types."""

import logging
import os
import tempfile
from typing import Dict

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..config.models import GtfsRtConfig
from ..schema.conform import conform
from .paths import aggregated_root  # noqa: F401 (imported from here before 0.7.4)
from .service import service_feeds
from ..storage.base import StorageInterface, storage_for

logger = logging.getLogger(__name__)


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
            storage = storage_for(storages, provider.name)
            tz = pytz.timezone(provider.timezone)
            for service in sorted(service_feeds(provider)):
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


def compact_old_days(
    config: GtfsRtConfig, storages: Dict[str, StorageInterface]
) -> None:
    """
    Compact the stored days older than the ones the pipeline compacts itself
    (the last COMPACTION_DAYS_BACK days), e.g. days stored before compact_daily
    was on. Streams one day at a time, as compact_daily does.
    """
    from .service import COMPACTION_DAYS_BACK, AggregatorService, service_feeds

    aggregator = AggregatorService(config, storages)
    for provider in config.providers:
        for service, feeds in service_feeds(provider).items():
            aggregator.compact_once(
                provider.name,
                [service],
                provider.timezone,
                feeds[0].deduplicate,
                days_back=None,
                skip_days=COMPACTION_DAYS_BACK,
            )
