from datetime import datetime, timedelta
from io import BytesIO
from typing import Dict, List, Any, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..config.models import GtfsRtConfig
from ..storage.base import StorageInterface
from ..utils.log_helper import setup_logger
from ..utils.serializer import ParquetSerializer

# Name of individual files, in the provider timezone, e.g. 2026-10-25_02-30-00+0100.
# Files written before 0.3.0 have no UTC offset.
INDIVIDUAL_TIME_FORMAT = "%Y-%m-%d_%H-%M-%S%z"
LEGACY_INDIVIDUAL_TIME_FORMAT = "%Y-%m-%d_%H-%M-%S"


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
                }

                self.logger.debug(
                    f"Created schedule for provider {provider.name}, check interval {check_interval}s"
                )
                name = f"Aggregator - {provider.name} - {api.services} - {api.frequency_minutes}m"
                # Add to schedules
                schedules.append((check_interval, self.run_once, name, args))

        self.logger.info(f"Created {len(schedules)} aggregation schedules")
        return schedules

    def run_once(
        self,
        provider_name: str,
        service_types: List[str],
        frequency_minutes: int,
        timezone: str,
    ):
        """
        Run an aggregation job once.

        Args:
            provider_name: Name of the provider
            service_types: List of service types to aggregate
            frequency_minutes: Frequency in minutes for grouping
            timezone: Timezone of the provider
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

        try:
            # Read all files
            table = None

            for file_path in files:
                # Read the file
                logger.debug(f"Reading file: {file_path}")
                data = storage.read_bytes(file_path)

                if table is None:
                    table = pq.read_table(BytesIO(data))
                    if table.num_rows <= 0:
                        logger.warning(
                            f"Empty DataFrame for {provider_name}/{service_type} at {group_time}"
                        )
                        table = None
                else:
                    try:
                        table = pa.concat_tables(
                            [table, pq.read_table(BytesIO(data))],
                            unicode_promote_options="default",
                        )
                    except Exception as e:
                        logger.error(
                            f"Error concatenating tables: {str(e)}", exc_info=True
                        )
                        error_files.append(file_path)

            logger.debug(
                f"Combined DataFrame has {round(table.num_rows / len(files))} records on average, for {len(files)} files"
            )

            # Create path for the grouped file
            group_time_str = group_time.strftime(self.config.output.time_format)
            next_period_str = next_period.strftime(self.config.output.time_format)
            day_str = group_time.strftime("%Y-%m-%d")
            filename = self.config.output.filename_format.format(
                group_time=group_time_str, next_period=next_period_str
            )
            path = f"{provider_name}/{service_type}/{day_str}/{filename}"

            # Add to an existing file instead of replacing it: files can arrive
            # after their period was aggregated (a slow fetch), and when clocks
            # go back both passes of the repeated hour share the same file name
            if storage.file_exists(path):
                logger.info(f"Adding {len(files)} files to existing {path}")
                table = pa.concat_tables(
                    [pq.read_table(BytesIO(storage.read_bytes(path))), table],
                    promote_options="default",
                )

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

    def _extract_datetime_from_filename(
        self, filename: str, timezone: Optional[pytz.BaseTzInfo] = None
    ) -> Optional[datetime]:
        """
        Extract the local fetch time from an individual file name.

        Args:
            filename: Filename to extract datetime from
            timezone: Provider timezone, to convert names that carry a UTC offset

        Returns:
            Datetime in the provider timezone (naive for names without offset),
            or None if the name does not match
        """
        basename = (
            filename.split("/")[-1].replace("individual_", "").replace(".parquet", "")
        )
        try:
            dt = datetime.strptime(basename, INDIVIDUAL_TIME_FORMAT)
            # Keep the offset: it tells the two passes of the hour that repeats
            # when clocks go back apart
            return dt.astimezone(timezone) if timezone is not None else dt
        except ValueError:
            pass
        try:
            return datetime.strptime(basename, LEGACY_INDIVIDUAL_TIME_FORMAT)
        except ValueError as e:
            self.logger.error(
                f"Error extracting datetime from filename: {str(e)}", stack_info=True
            )
            return None

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
