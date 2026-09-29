import json
import os
import posixpath
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

import pyarrow.parquet as pq
import pytz

from . import fetch_times
from ..aggregator.compaction import compact_files, sorted_by, write_sorted
from ..aggregator.dedup import deduplicate as deduplicate_rows
from ..aggregator.paths import aggregated_root, day_files, day_folder
from ..config.models import GtfsRtConfig
from ..schema.conform import conform
from ..storage.base import StorageInterface, storage_for
from ..utils.log_helper import setup_logger
from ..utils.file_time import parse_file_time

# Parquet metadata key listing the files merged into an aggregated or
# compacted file ("name:size"): if the process stops after writing it but
# before deleting them, they are recognised and not merged twice
SOURCES_METADATA = b"gtfs_rt_aggregator.sources"


def _source_key(path: str, local_path: str) -> str:
    return f"{posixpath.basename(path)}:{os.path.getsize(local_path)}"


def _sources(local_path: str) -> set:
    metadata = pq.read_schema(local_path).metadata or {}
    value = metadata.get(SOURCES_METADATA)
    return set(json.loads(value)) if value else set()


def service_feeds(provider) -> Dict[str, list]:
    """Realtime feeds of a provider by service type, in configuration order."""
    feeds: Dict[str, list] = {}
    for api in provider.realtime:
        for service in api.services:
            feeds.setdefault(service, []).append(api)
    return feeds


# How long after its end a period is aggregated even without a file from the
# next period
PERIOD_GRACE_SECONDS = 300
# Oldest day looked for when compacting every stored day (20 years)
MAX_DAYS_BACK = 20 * 366
# Finished days the pipeline compacts (again if files were added)
COMPACTION_DAYS_BACK = 7


class AggregatorService:
    """Service for aggregating GTFS-RT data."""

    def __init__(self, config: GtfsRtConfig, storages: Dict[str, StorageInterface]):
        """
        Initialize the aggregator service.

        Args:
            config: Configuration
            storages: Dictionary of storage interfaces by provider name, with 'global' as the default
        """
        self.logger = setup_logger(f"{__name__}.AggregatorService")
        self.logger.debug("Initializing aggregator service")
        self.config = config
        self.storages = storages
        self.logger.debug("Aggregator service initialized")

    def get_scheduling(self) -> List[Tuple[Any, callable, str, Dict[str, Any]]]:
        """
        Jobs of the aggregator: one aggregation (and one compaction, with
        compact_daily) per provider and service type. Several feeds of a
        service share its files, and its settings (checked by ProviderConfig).

        Returns:
            (interval in seconds, function, name, arguments, service) tuples,
            service being "<provider>/<service type>"
        """
        schedules = []
        for provider in self.config.providers:
            for service, feeds in service_feeds(provider).items():
                api = feeds[0]
                args = {
                    "provider_name": provider.name,
                    "service_types": [service],
                    "frequency_minutes": api.frequency_minutes,
                    "timezone": provider.timezone,
                    "deduplicate": api.deduplicate,
                }
                interval = min(feed.check_interval_seconds for feed in feeds)
                name = f"Aggregator - {provider.name} - {service}"
                schedules.append(
                    (interval, self.run_once, name, args, f"{provider.name}/{service}")
                )
                if self.config.output.compact_daily:
                    compact_args = {
                        "provider_name": provider.name,
                        "service_types": [service],
                        "timezone": provider.timezone,
                        "deduplicate": api.deduplicate,
                    }
                    schedules.append(
                        (
                            24 * 3600,
                            self.compact_once,
                            f"Compaction - {provider.name} - {service}",
                            compact_args,
                            f"{provider.name}/{service}",
                        )
                    )
        self.logger.info(f"Created {len(schedules)} aggregation schedules")
        return schedules

    def run_once(
        self,
        provider_name: str,
        service_types: List[str],
        frequency_minutes: int,
        timezone: str,
        deduplicate: bool = False,
    ):
        """
        Run an aggregation job once.

        Args:
            provider_name: Name of the provider
            service_types: List of service types to aggregate
            frequency_minutes: Frequency in minutes for grouping
            timezone: Timezone of the provider
            deduplicate: Merge consecutive identical rows (firstSeen / lastSeen)
        """
        job_logger = setup_logger(f"{__name__}.AggregatorService.job.{provider_name}")
        job_logger.info(
            f"Starting aggregation job for {provider_name}, service types: {service_types}"
        )

        try:
            # Get timezone
            tz = pytz.timezone(timezone)

            # Process each service type
            for service_type in service_types:
                job_logger.debug(f"Aggregating service type: {service_type}")
                self._aggregate_service_type(
                    provider_name=provider_name,
                    service_type=service_type,
                    frequency_minutes=frequency_minutes,
                    timezone=tz,
                    logger=job_logger,
                    deduplicate=deduplicate,
                )

            job_logger.info(f"Completed aggregation job for {provider_name}")
        except Exception as e:
            job_logger.error(f"Error in aggregation job: {str(e)}", exc_info=True)

    def _get_storage_for_provider(self, provider_name: str) -> StorageInterface:
        """
        Get the storage interface for a provider.

        Args:
            provider_name: Name of the provider

        Returns:
            Storage interface for the provider
        """
        return storage_for(self.storages, provider_name)

    def _aggregate_service_type(
        self,
        provider_name: str,
        service_type: str,
        frequency_minutes: int,
        timezone: pytz.timezone,
        logger=None,
        deduplicate: bool = False,
    ):
        """
        Aggregate a service type.

        Args:
            provider_name: Name of the provider
            service_type: Service type to aggregate
            frequency_minutes: Frequency in minutes for grouping
            timezone: Timezone of the provider
            logger: Logger to use
        """
        logger = logger or self.logger

        # Get the storage for this provider
        storage = self._get_storage_for_provider(provider_name)

        # List all individual files
        directory = f"{provider_name}/{service_type}/individual/"
        logger.debug(f"Listing individual files in {directory}")
        files = storage.list_files(directory, "*.parquet")

        if not files:
            logger.info(f"No individual files found {directory}")
            return

        logger.debug(f"Found {len(files)} individual files for {directory}")

        # Group files by rounded time
        logger.debug(
            f"Grouping files by time with frequency {frequency_minutes} minutes"
        )
        grouped_files = self._group_files_by_time(files, frequency_minutes, timezone)

        logger.debug(f"Created {len(grouped_files)} time groups")

        # Most recent period with a file
        latest = max(grouped_files) if grouped_files else None

        # Process each group
        for group_time, group_files in grouped_files.items():
            if not group_files:
                continue

            logger.debug(
                f"Processing group at {group_time} with {len(group_files)} files"
            )

            # Get the next time period, in local time
            next_period = self._localize(
                group_time.replace(tzinfo=None) + timedelta(minutes=frequency_minutes),
                group_time,
            )

            # Wait for a file from the next time period. A feed that stopped
            # changing produces none (unchanged fetches are not stored): close
            # the period anyway once it is well over. Files arriving later are
            # added to its file.
            if latest < next_period and datetime.now(
                timezone
            ) < next_period + timedelta(seconds=PERIOD_GRACE_SECONDS):
                logger.info(
                    f"Skipping group {group_time} for {service_type} - no files from next period yet"
                )
                continue

            # Aggregate the files
            logger.debug(f"Aggregating {len(group_files)} files for group {group_time}")
            self._aggregate_files(
                provider_name=provider_name,
                service_type=service_type,
                files=group_files,
                group_time=group_time,
                next_period=next_period,
                storage=storage,
                logger=logger,
                deduplicate=deduplicate,
            )

    def _group_files_by_time(
        self, files: List[str], frequency_minutes: int, timezone: pytz.timezone
    ) -> Dict[datetime, List[str]]:
        """
        Group files by rounded time.

        Args:
            files: List of files
            frequency_minutes: Frequency in minutes for grouping
            timezone: Timezone for the files

        Returns:
            Dictionary with rounded times as keys and lists of files as values
        """
        self.logger.debug(
            f"Grouping {len(files)} files by {frequency_minutes} minute intervals"
        )
        grouped_files = {}

        for file_path in files:
            # Extract datetime from filename
            file_dt = self._extract_datetime_from_filename(file_path, timezone)
            if not file_dt:
                self.logger.warning(
                    f"Could not extract datetime from filename: {file_path}"
                )
                continue

            # Localize the datetime
            file_dt = timezone.localize(file_dt) if file_dt.tzinfo is None else file_dt

            # Round down to the nearest frequency
            rounded_time = self._get_rounded_time(file_dt, frequency_minutes)

            # Group files by the rounded time
            if rounded_time not in grouped_files:
                grouped_files[rounded_time] = []
            grouped_files[rounded_time].append(file_path)

        # Log the groups
        for rounded_time, group_files in grouped_files.items():
            self.logger.debug(f"Group {rounded_time}: {len(group_files)} files")

        return grouped_files

    def _aggregate_files(
        self,
        provider_name: str,
        service_type: str,
        files: List[str],
        group_time: datetime,
        next_period: datetime,
        storage: StorageInterface,
        logger=None,
        deduplicate: bool = False,
    ):
        """
        Merge the individual files of a period into its aggregated file, then
        delete them. Files that cannot be read are moved to error/.

        Args:
            provider_name: Name of the provider
            service_type: Service type
            files: Individual files of the period
            group_time: Start of the period
            next_period: End of the period
            storage: Storage interface to use
            logger: Logger to use
            deduplicate: Merge consecutive identical rows (firstSeen / lastSeen)
        """
        logger = logger or self.logger
        logger.info(
            f"Aggregating {len(files)} files for {provider_name}/{service_type} at {group_time}"
        )
        path = self.output_path(provider_name, service_type, group_time, next_period)
        try:
            # Added to an existing file instead of replacing it: files can
            # arrive after their period was aggregated (a slow fetch), and when
            # clocks go back both passes of the repeated hour share the same
            # file name
            merged = self._merge_files(
                storage,
                files,
                path,
                service_type,
                provider_name,
                group_time.tzinfo,
                deduplicate,
                logger,
            )
        except Exception as e:
            logger.error(f"Error aggregating files into {path}: {e}", exc_info=True)
            return

        for file_path in merged["failed"]:
            storage.rename_file(file_path, file_path.replace("individual", "error"))
        for file_path in merged["merged"] + merged["skipped"]:
            if storage.delete_file(file_path) is False:
                logger.error(f"Could not delete aggregated file {file_path}")
        logger.info(
            f"Grouped {len(merged['merged'])} files with {merged['rows']} records to {path}"
        )

    def output_path(
        self, provider_name: str, service_type: str, start: datetime, end: datetime
    ) -> str:
        """Path of the aggregated file of a period, from output.path_template."""
        return self.config.output.path_template.format(
            provider=provider_name, service=service_type, start=start, end=end
        )

    def compact_once(
        self,
        provider_name: str,
        service_types: List[str],
        timezone: str,
        deduplicate: bool = False,
        days_back: Optional[int] = COMPACTION_DAYS_BACK,
        skip_days: int = 0,
    ):
        """
        Merge the aggregated files of each finished day into one file, sorted by
        output.sort_by. Looks at the last days_back days before today (every
        day stored if None), except the skip_days most recent ones; a day
        compacted earlier is compacted again if files were added to it since.

        Args:
            provider_name: Name of the provider
            service_types: Service types to compact
            timezone: Timezone of the provider
            deduplicate: Merge consecutive identical rows (firstSeen / lastSeen)
            days_back: How many finished days to look at (None: all)
            skip_days: How many of the most recent finished days to leave
        """
        logger = setup_logger(f"{__name__}.AggregatorService.compact.{provider_name}")
        tz = pytz.timezone(timezone)
        storage = self._get_storage_for_provider(provider_name)
        today = datetime.now(tz).date()
        name = self.config.output.compacted_name

        for service_type in service_types:
            last = days_back
            if last is None:
                last = self._oldest_day_back(
                    storage, provider_name, service_type, tz, today
                )
            for back in range(skip_days + 1, last + 1):
                day = today - timedelta(days=back)
                folder = day_folder(self.config, provider_name, service_type, day, tz)
                try:
                    files = day_files(storage, folder)
                    compacted = posixpath.join(folder, name)
                    parts = [f for f in files if f != compacted]
                    if not parts:
                        continue
                    merged = self._merge_files(
                        storage,
                        parts,
                        compacted,
                        service_type,
                        provider_name,
                        tz,
                        deduplicate,
                        logger,
                    )
                    for f in merged["merged"] + merged["skipped"]:
                        if storage.delete_file(f) is False:
                            # Left in place, it is recognised next time (sources)
                            logger.error(f"Could not delete compacted file {f}")
                    for f in merged["failed"]:
                        logger.error(f"Could not read {f}: left out of {compacted}")
                    logger.info(
                        f"Compacted {len(merged['merged'])} files into {compacted} ({merged['rows']} rows)"
                    )
                except Exception as e:
                    logger.error(f"Error compacting {folder}: {e}", exc_info=True)

    def _oldest_day_back(self, storage, provider_name, service_type, tz, today) -> int:
        """How many days before today the oldest stored day folder is (0 if none)."""
        root = aggregated_root(self.config, provider_name, service_type)
        folders = {
            posixpath.dirname(p)
            for p in storage.walk_files(root)
            if p.endswith(".parquet")
        }
        oldest = 0
        # Day folders are found by rendering path_template for each day back
        for back in range(1, MAX_DAYS_BACK + 1):
            if not folders:
                break
            day = today - timedelta(days=back)
            folder = day_folder(self.config, provider_name, service_type, day, tz)
            if folder in folders:
                folders.discard(folder)
                oldest = back
        return oldest

    def _merge_files(
        self,
        storage,
        paths: List[str],
        output: str,
        service_type: str,
        provider_name: str,
        tz,
        deduplicate: bool,
        logger,
    ) -> Dict[str, Any]:
        """
        Merge files into output (with what output already holds), sorted by
        output.sort_by (entityId, firstSeen when deduplicating), streaming (see
        compaction.py): memory stays around one input file, whatever the
        size of the output. The output records the fetch times of its rows
        (see fetch_times.py) and the files merged into it.

        Returns rows (written), merged (paths merged), skipped (paths already
        in output) and failed (paths that could not be read). An existing
        output that cannot be read raises: it is never replaced.
        """
        with tempfile.TemporaryDirectory(prefix="gtfs_rt_aggregator-merge-") as tmp:
            # (path, local copy); the output first, if it exists
            inputs, done = [], set()
            if storage.file_exists(output):
                local = os.path.join(tmp, "previous.parquet")
                storage.read_to_file(output, local)
                done = _sources(local)
                inputs.append((output, local))

            # Source key ("name:size") of each file merged, or already merged
            keys_of: Dict[str, str] = {}
            skipped, failed = [], []
            for index, path in enumerate(paths):
                local = os.path.join(tmp, f"in-{index}.parquet")
                try:
                    storage.read_to_file(path, local)
                    pq.read_schema(local)
                except Exception as e:
                    logger.error(f"Could not read {path}: {e}")
                    failed.append(path)
                    continue
                keys_of[path] = _source_key(path, local)
                if keys_of[path] in done:
                    # Merged by a run stopped before deleting it
                    skipped.append(path)
                    os.remove(local)
                else:
                    inputs.append((path, local))

            if deduplicate:
                keys = ["entityId", "firstSeen"]
            else:
                # Sort columns present in every file
                names = [set(pq.read_schema(local).names) for _, local in inputs]
                keys = [
                    c for c in self.config.output.sort_by if all(c in n for n in names)
                ]

            merged, sorted_paths, times = [], [], {}
            for index, (path, local) in enumerate(inputs):
                if sorted_by(local) == keys:
                    # Written since 0.6.0 with the same settings: already sorted
                    fetch_times.of_file(local, times)
                    sorted_paths.append(local)
                else:
                    try:
                        # One file in memory at a time
                        table = pq.read_table(local)
                        fetch_times.merge(
                            times, fetch_times.decode(table.schema.metadata)
                        )
                        table = conform(table, service_type, provider_name, tz)
                    except Exception as e:
                        if path == output:
                            raise
                        logger.error(f"Could not read {path}: {e}")
                        failed.append(path)
                        continue
                    fetch_times.from_table(table, times)
                    if deduplicate:
                        table = deduplicate_rows(table, fetch_times.to_arrays(times))
                    prepared = os.path.join(tmp, f"sorted-{index}.parquet")
                    write_sorted(table, keys, prepared)
                    del table
                    os.remove(local)
                    sorted_paths.append(prepared)
                if path != output:
                    merged.append(path)
            if not merged:
                return {"rows": 0, "merged": [], "skipped": skipped, "failed": failed}

            # Skipped files are listed again: their deletion may fail again
            sources = sorted(keys_of[p] for p in merged + skipped)
            result = os.path.join(tmp, "out.parquet")
            rows = compact_files(
                sorted_paths,
                keys,
                result,
                deduplicate_rows=deduplicate,
                times=fetch_times.to_arrays(times) if deduplicate else None,
                metadata={
                    **fetch_times.encode(times),
                    SOURCES_METADATA: json.dumps(sources).encode(),
                },
            )
            storage.save_file(result, output)
            return {
                "rows": rows,
                "merged": merged,
                "skipped": skipped,
                "failed": failed,
            }

    def _extract_datetime_from_filename(
        self, filename: str, timezone: Optional[pytz.BaseTzInfo] = None
    ) -> Optional[datetime]:
        """
        Extract the local fetch time from an individual file name.

        Args:
            filename: Filename to extract datetime from
            timezone: Provider timezone

        Returns:
            Datetime in the provider timezone (naive for names from before 0.3.0),
            or None if the name does not match
        """
        basename = (
            filename.split("/")[-1].replace("individual_", "").replace(".parquet", "")
        )
        dt = parse_file_time(basename)
        if dt is None:
            self.logger.error(f"Could not extract a datetime from filename: {filename}")
            return None
        # Keep the real instant: it tells the two passes of the hour that
        # repeats when clocks go back apart
        if dt.tzinfo is not None and timezone is not None:
            return dt.astimezone(timezone)
        return dt

    @staticmethod
    def _get_rounded_time(dt: datetime, freq_minutes: int) -> datetime:
        """
        Round a datetime down to the nearest frequency.

        Args:
            dt: Datetime to round
            freq_minutes: Frequency in minutes

        Returns:
            Rounded datetime
        """
        # Round down to the nearest frequency, in local time
        minutes = (dt.hour * 60 + dt.minute) // freq_minutes * freq_minutes
        rounded = dt.replace(
            hour=minutes // 60, minute=minutes % 60, second=0, microsecond=0
        )
        return AggregatorService._localize(rounded.replace(tzinfo=None), dt)

    @staticmethod
    def _localize(naive: datetime, reference: datetime) -> datetime:
        """
        Attach the timezone of an aware reference datetime to a naive local time.

        dt.replace(hour=...) on a pytz datetime keeps the UTC offset of the
        original time, which is wrong when the clocks changed in between (e.g.
        midnight on the day clocks go back). An ambiguous time takes the same
        side of the change as the reference.
        """
        if reference.tzinfo is None:
            return naive
        localize = getattr(reference.tzinfo, "localize", None)
        if localize is None:
            # Not a pytz timezone (e.g. zoneinfo): replace() is already correct
            return naive.replace(tzinfo=reference.tzinfo)
        return localize(naive, is_dst=bool(reference.dst()))
