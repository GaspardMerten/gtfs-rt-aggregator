from datetime import datetime
from typing import Dict, List, Any, Tuple

import pytz

from gtfs_rt_aggregator.config.models import GtfsRtConfig
from gtfs_rt_aggregator.fetcher.gtfs_rt import GtfsRtFetcher
from gtfs_rt_aggregator.storage.base import StorageInterface
from gtfs_rt_aggregator.utils.log_helper import setup_logger
from gtfs_rt_aggregator.utils.serializer import ParquetSerializer


class FetcherService:
    """Service for fetching GTFS-RT data."""

    def __init__(self, config: GtfsRtConfig, storages: Dict[str, StorageInterface]):
        """
        Initialize the fetcher service.

        @param config: Configuration
        @param storages: Dictionary of storage interfaces by provider name, with 'global' as the default
        """
        self.logger = setup_logger(f"{__name__}.FetcherService")
        self.logger.debug("Initializing fetcher service")
        self.config = config
        self.storages = storages
        self.logger.debug("Fetcher service initialized")

    def get_scheduling(self) -> List[Tuple[Any, callable, str, Dict[str, Any]]]:
        """
        Get the scheduling configuration for the fetcher service.

        Returns:
            List of tuples containing (schedule job, function, arguments)
        """
        self.logger.debug("Creating fetch schedules")
        schedules = []

        # Create schedules for each provider and API
        for provider in self.config.providers:
            for api in provider.apis:
                # Create the function arguments
                args = {
                    "provider_name": provider.name,
                    "url": api.url,
                    "service_types": api.services,
                    "timezone": provider.timezone,
                }

                self.logger.debug(
                    f"Created schedule for provider {provider.name}, API {api.url}, refresh {api.refresh_seconds}s"
                )

                name = (
                    "Fetcher - "
                    + provider.name
                    + " - "
                    + api.url
                    + " - "
                    + str(api.services)
                )

                # Add to schedules
                schedules.append((api.refresh_seconds, self.run_once, name, args))

        self.logger.info(f"Created {len(schedules)} fetch schedules")
        return schedules

    def run_once(
        self, provider_name: str, url: str, service_types: List[str], timezone: str
    ):
        """
        Run a fetch job once.

        @param provider_name: Name of the provider
        @param url: URL of the GTFS-RT feed
        @param service_types: List of service types to fetch
        @param timezone: Timezone of the provider
        """
        job_logger = setup_logger(f"{__name__}.FetcherService.job.{provider_name}")
        job_logger.info(f"Starting fetch job for {provider_name} from {url}")

        try:
            # Get the storage for this provider
            storage = self._get_storage_for_provider(provider_name)

            # Get timezone
            tz = pytz.timezone(timezone)

            # Fetch time
            fetch_time = datetime.now(tz)
            job_logger.debug(f"Fetch time: {fetch_time}")

            # Fetch and parse data
            job_logger.debug(f"Fetching data for service types: {service_types}")
            result = GtfsRtFetcher.fetch_and_parse(url, service_types, timezone)

            # Save each service type
            for service_type, df in result.items():
                if service_type not in service_types:
                    job_logger.warning(
                        f"Service type {service_type} not in service types {service_types}"
                    )
                    continue
                if df.empty:
                    job_logger.debug(f"No data for service type {service_type}")
                    continue

                job_logger.debug(
                    f"Processing {len(df)} records for service type {service_type}"
                )

                # Convert to Parquet bytes
                parquet_bytes = ParquetSerializer.dataframe_to_parquet_bytes(df)

                # Create path
                filename = (
                    f"individual/{fetch_time.strftime('%Y-%m-%d_%H-%M-%S')}.parquet"
                )
                path = f"{provider_name}/{service_type}/{filename}"

                # Save to storage
                job_logger.debug(f"Saving data to {path}")
                saved_path = storage.save_bytes(parquet_bytes, path)

                job_logger.info(
                    f"Saved {service_type} data with {len(df)} records to {saved_path}"
                )

        except Exception as e:
            job_logger.error(f"Error in fetch job: {str(e)}", exc_info=True)

    def _get_storage_for_provider(self, provider_name: str) -> StorageInterface:
        """
        Get the storage interface for a provider.

        @param provider_name: Name of the provider
        @return Storage interface for the provider
        """
        # Use provider-specific storage if available, otherwise use global
        storage = self.storages.get(provider_name, self.storages["global"])
        self.logger.debug(
            f"Using {'provider-specific' if provider_name in self.storages else 'global'} storage for provider {provider_name}"
        )
        return storage
