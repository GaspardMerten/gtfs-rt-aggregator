import os
import posixpath
import tempfile
from datetime import datetime, timedelta
from io import BytesIO
from typing import Dict, List, Any, Optional, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytz

from ..aggregator.compaction import compact_files, sorted_by, write_sorted
from ..aggregator.dedup import deduplicate as deduplicate_rows
from ..config.models import GtfsRtConfig
from ..schema.conform import conform
from ..storage.base import StorageInterface
from ..utils.log_helper import setup_logger
from ..utils.file_time import parse_file_time
from ..utils.serializer import ParquetSerializer

TIMESTAMP = pa.timestamp("us", tz="UTC")


def _fetch_times(path: str) -> set:
    """Every fetch time found in a file (fetchTime, firstSeen, lastSeen)."""
    times = set()
    names = [
        c
        for c in ("fetchTime", "firstSeen", "lastSeen")
        if c in pq.read_schema(path).names
    ]
    for batch in pq.ParquetFile(path).iter_batches(columns=names, batch_size=262_144):
        for name in names:
            times.update(pc.unique(batch.column(name)).to_pylist())
    times.discard(None)
    return times


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
        Get the scheduling configuration for the aggregator service.

        Returns:
            List of tuples containing (schedule job, function, arguments)
        """
        self.logger.debug("Creating aggregation schedules")
        schedules = []

        # Create schedules for each provider and API
        for provider in self.config.providers:
            for api in provider.realtime:
                # Get the check interval
                check_interval = api.check_interval_seconds

                # Create the function arguments
                args = {
                    "provider_name": provider.name,
                    "service_types": api.services,
                    "frequency_minutes": api.frequency_minutes,
                    "timezone": provider.timezone,
                    "deduplicate": api.deduplicate,
                }

                self.logger.debug(
                    f"Created schedule for provider {provider.name}, check interval {check_interval}s"
                )
                name = f"Aggregator - {provider.name} - {api.services} - {api.frequency_minutes}m"
                # Add to schedules
                schedules.append((check_interval, self.run_once, name, args))

                if self.config.output.compact_daily:
                    compact_args = {
                        "provider_name": provider.name,
                        "service_types": api.services,
                        "timezone": provider.timezone,
                        "deduplicate": api.deduplicate,
                    }
                    schedules.append(
                        (
                            24 * 3600,
                            self.compact_once,
                            f"Compaction - {provider.name} - {api.services}",
                            compact_args,
                            True,
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
        # Use provider-specific storage if available, otherwise use global
        storage = self.storages.get(provider_name, self.storages["global"])
        self.logger.debug(
            f"Using {'provider-specific' if provider_name in self.storages else 'global'} storage for provider {provider_name}"
        )
        return storage

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

            # Check if there's at least one file from the next time period
            has_next_period_file = False
            for file_path in files:
                file_dt = self._extract_datetime_from_filename(file_path, timezone)
                if file_dt:
                    file_dt = (
                        timezone.localize(file_dt)
                        if file_dt.tzinfo is None
                        else file_dt
                    )
                    rounded_time = self._get_rounded_time(file_dt, frequency_minutes)
                    if rounded_time >= next_period:
                        has_next_period_file = True
                        break

            # A feed that stopped changing produces no file for the next period
            # (unchanged fetches are not stored): close the period anyway once
            # it is well over. Files arriving later are added to its file.
            if not has_next_period_file and datetime.now(
                timezone
            ) >= next_period + timedelta(seconds=PERIOD_GRACE_SECONDS):
                has_next_period_file = True

            if not has_next_period_file:
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
        Aggregate files into a single file.

        Args:
            provider_name: Name of the provider
            service_type: Service type
            files: List of files to aggregate
            group_time: Group time
            storage: Storage interface to use
            logger: Logger to use
        """
        logger = logger or self.logger
        logger.info(
            f"Aggregating {len(files)} files for {provider_name}/{service_type} at {group_time}"
        )

        error_files = []

        def read(data: bytes) -> pa.Table:
            # Files from earlier versions get the current schema
            return conform(
                pq.read_table(BytesIO(data)),
                service_type,
                provider_name,
                group_time.tzinfo,
            )

        try:
            # Read all files
            table = None

            for file_path in files:
                # Read the file
                logger.debug(f"Reading file: {file_path}")
                data = storage.read_bytes(file_path)

                if table is None:
                    table = read(data)
                    if table.num_rows <= 0:
                        logger.warning(
                            f"Empty DataFrame for {provider_name}/{service_type} at {group_time}"
                        )
                        table = None
                else:
                    try:
                        table = pa.concat_tables(
                            [table, read(data)],
                            # Files from different versions may have different columns
                            promote_options="default",
                        )
                    except Exception as e:
                        logger.error(
                            f"Error concatenating tables: {str(e)}", exc_info=True
                        )
                        error_files.append(file_path)

            logger.debug(
                f"Combined DataFrame has {round(table.num_rows / len(files))} records on average, for {len(files)} files"
            )

            path = self.output_path(
                provider_name, service_type, group_time, next_period
            )

            # Add to an existing file instead of replacing it: files can arrive
            # after their period was aggregated (a slow fetch), and when clocks
            # go back both passes of the repeated hour share the same file name
            if storage.file_exists(path):
                logger.info(f"Adding {len(files)} files to existing {path}")
                table = pa.concat_tables(
                    [read(storage.read_bytes(path)), table],
                    promote_options="default",
                )

            if deduplicate:
                before = table.num_rows
                table = deduplicate_rows(table)
                logger.info(f"Deduplicated {before} rows into {table.num_rows}")

            # Convert to Parquet bytes
            logger.debug("Converting combined DataFrame to Parquet")
            parquet_bytes = ParquetSerializer.pyarrow_table_to_bytes(table)

            # Save to storage
            logger.debug(f"Saving grouped file to {path}")
            saved_path = storage.save_bytes(parquet_bytes, path)

            logger.info(
                f"Grouped {len(files)} files with {table.num_rows} records to {saved_path}"
            )

            # Delete individual files
            logger.debug(f"Deleting {len(files)} individual files")

            for file_path in files:
                if file_path in error_files:
                    storage.rename_file(
                        file_path, file_path.replace("individual", "error")
                    )
                else:
                    storage.delete_file(file_path)
                logger.debug(f"Removed individual file: {file_path}")

        except Exception as e:
            logger.error(f"Error aggregating files: {str(e)}", exc_info=True)

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
                start = tz.localize(datetime(day.year, day.month, day.day))
                folder = posixpath.dirname(
                    self.output_path(provider_name, service_type, start, start)
                )
                try:
                    files = sorted(
                        f
                        for f in storage.list_files(folder, "*.parquet")
                        # Some backends also list files in subfolders
                        if posixpath.dirname(f) == folder
                    )
                    compacted = posixpath.join(folder, name)
                    parts = [f for f in files if posixpath.basename(f) != name]
                    if not parts:
                        continue
                    rows = self._compact_day(
                        storage,
                        files,
                        name,
                        compacted,
                        service_type,
                        provider_name,
                        tz,
                        deduplicate,
                    )
                    for f in parts:
                        if storage.delete_file(f) is False:
                            # Left in place, it would be counted twice next time
                            logger.error(f"Could not delete compacted file {f}")
                    logger.info(
                        f"Compacted {len(parts)} files into {compacted} ({rows} rows)"
                    )
                except Exception as e:
                    logger.error(f"Error compacting {folder}: {e}", exc_info=True)

    def _oldest_day_back(self, storage, provider_name, service_type, tz, today) -> int:
        """How many days before today the oldest stored day folder is (0 if none)."""
        from .convert import aggregated_root

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
            start = tz.localize(datetime(day.year, day.month, day.day))
            folder = posixpath.dirname(
                self.output_path(provider_name, service_type, start, start)
            )
            if folder in folders:
                folders.discard(folder)
                oldest = back
        return oldest

    def _compact_day(
        self,
        storage,
        files,
        name,
        compacted,
        service_type,
        provider_name,
        tz,
        deduplicate,
    ) -> int:
        """
        Merge a day's files into one sorted file, streaming (see compaction.py):
        memory stays around one hourly file, whatever the size of the day.
        """
        with tempfile.TemporaryDirectory(prefix="gtfs_rt_aggregator-compact-") as tmp:
            downloaded = []
            for index, path in enumerate(files):
                local = os.path.join(tmp, f"in-{index}.parquet")
                storage.read_to_file(path, local)
                downloaded.append((path, local))

            if deduplicate:
                keys = ["entityId", "firstSeen"]
            else:
                # Sort columns present in every file
                names = [set(pq.read_schema(local).names) for _, local in downloaded]
                keys = [
                    c for c in self.config.output.sort_by if all(c in n for n in names)
                ]

            sorted_paths, times = [], set()
            for index, (path, local) in enumerate(downloaded):
                if posixpath.basename(path) == name and sorted_by(local) == keys:
                    # Compacted since 0.6.0 with the same settings: already sorted
                    sorted_paths.append(local)
                else:
                    # One file in memory at a time
                    table = conform(
                        pq.read_table(local), service_type, provider_name, tz
                    )
                    if deduplicate:
                        table = deduplicate_rows(table)
                    prepared = os.path.join(tmp, f"sorted-{index}.parquet")
                    write_sorted(table, keys, prepared)
                    del table
                    os.remove(local)
                    sorted_paths.append(prepared)
                if deduplicate:
                    times.update(_fetch_times(sorted_paths[-1]))

            output = os.path.join(tmp, "out.parquet")
            rows = compact_files(
                sorted_paths,
                keys,
                output,
                deduplicate_rows=deduplicate,
                times=pa.array(sorted(times), TIMESTAMP) if deduplicate else None,
            )
            storage.save_file(output, compacted)
            return rows

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
