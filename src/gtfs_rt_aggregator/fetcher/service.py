from datetime import datetime
from io import BytesIO
from multiprocessing import Manager
from typing import Dict, List, Any, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..aggregator.service import AggregatorService
from ..config.models import GtfsRtConfig
from ..fetcher.gtfs_rt import GtfsRtFetcher
from ..storage.base import StorageInterface
from ..utils.log_helper import setup_logger
from ..utils.file_time import format_file_time
from ..utils.serializer import ParquetSerializer


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

        # Fetch jobs run in separate processes, so accumulated fetches live in a
        # Manager process. One buffer and lock per (provider, API, service type),
        # created up front so the jobs never need the Manager itself.
        self._manager = None
        self._accumulators = {}
        for provider in config.providers:
            for api in provider.realtime:
                if not api.accumulate_minutes:
                    continue
                if self._manager is None:
                    self._manager = Manager()
                for service_type in api.services:
                    key = self._accumulator_key(provider.name, api.url, service_type)
                    self._accumulators[key] = {
                        "buffer": self._manager.list(),
                        "lock": self._manager.Lock(),
                        "minutes": api.accumulate_minutes,
                        "concatenate": api.accumulate_concatenate,
                        "provider_name": provider.name,
                        "service_type": service_type,
                    }

        self.logger.debug("Fetcher service initialized")

    def __getstate__(self):
        # The Manager cannot be pickled (spawn/forkserver start methods pickle
        # the job target); its proxies can, and are all the jobs need.
        state = self.__dict__.copy()
        state["_manager"] = None
        return state

    @staticmethod
    def _accumulator_key(provider_name: str, url: str, service_type: str) -> str:
        return f"{provider_name}|{url}|{service_type}"

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
            for api in provider.realtime:
                # Create the function arguments
                args = {
                    "provider_name": provider.name,
                    "url": api.url,
                    "service_types": api.services,
                    "timezone": provider.timezone,
                    "headers": api.headers,
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
        self,
        provider_name: str,
        url: str,
        service_types: List[str],
        timezone: str,
        headers: Optional[Dict[str, str]] = None,
    ):
        """
        Run a fetch job once.

        @param provider_name: Name of the provider
        @param url: URL of the GTFS-RT feed
        @param service_types: List of service types to fetch
        @param timezone: Timezone of the provider
        @param headers: HTTP headers to send (e.g. an API key)
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
            result = GtfsRtFetcher.fetch_and_parse(
                url, service_types, timezone, headers
            )

            # Save each service type
            for service_type, df in result.items():
                if service_type not in service_types:
                    job_logger.warning(
                        f"Service type {service_type} not in service types {service_types}"
                    )
                    continue

                job_logger.debug(
                    f"Processing {len(df)} records for service type {service_type}"
                )

                # Convert to Parquet bytes
                parquet_bytes = ParquetSerializer.pyarrow_table_to_bytes(
                    df, compression="snappy"
                )

                filename = f"individual/{format_file_time(fetch_time)}.parquet"
                path = f"{provider_name}/{service_type}/{filename}"

                accumulator = self._accumulators.get(
                    self._accumulator_key(provider_name, url, service_type)
                )

                if accumulator is None:
                    job_logger.debug(f"Saving data to {path}")
                    saved_path = storage.save_bytes(parquet_bytes, path)
                    job_logger.info(
                        f"Saved {service_type} data with {len(df)} records to {saved_path}"
                    )
                    continue

                # Clock-aligned window (e.g. 16:00-16:15), in the provider timezone
                window = AggregatorService._get_rounded_time(
                    fetch_time, accumulator["minutes"]
                )
                item = (window, path, parquet_bytes)

                # Buffer and drain under the lock so concurrent jobs can neither
                # lose a fetch nor write the same batch twice. Nothing runs when
                # a window ends, so the first fetch of the next window writes it.
                batch = None
                with accumulator["lock"]:
                    buffer = accumulator["buffer"]
                    buffered_window = buffer[0][0] if len(buffer) else None
                    if buffered_window is not None and window < buffered_window:
                        # A slow job from an already written window: write it
                        # alone rather than mixing windows
                        batch = [item]
                    else:
                        if buffered_window is not None and window > buffered_window:
                            batch = buffer[:]
                            del buffer[:]
                        buffer.append(item)

                if batch is None:
                    job_logger.debug(
                        f"Buffered {service_type} data for {path} (window {window})"
                    )
                else:
                    self._write_batch(
                        storage, batch, accumulator["concatenate"], job_logger
                    )

        except Exception as e:
            job_logger.error(f"Error in fetch job: {str(e)}", exc_info=True)

    def flush_all(self):
        """
        Write every fetch still buffered in memory to storage.

        Meant to be called once the fetch jobs have stopped (e.g. on shutdown).
        """
        for key, accumulator in self._accumulators.items():
            try:
                # Jobs are stopped, but one may have been killed while holding
                # the lock: don't wait on it forever
                acquired = accumulator["lock"].acquire(timeout=5)
                try:
                    buffer = accumulator["buffer"]
                    batch = buffer[:]
                    del buffer[:]
                finally:
                    if acquired:
                        accumulator["lock"].release()

                if batch:
                    self.logger.info(
                        f"Flushing {len(batch)} buffered fetches for {key}"
                    )
                    storage = self._get_storage_for_provider(
                        accumulator["provider_name"]
                    )
                    self._write_batch(
                        storage, batch, accumulator["concatenate"], self.logger
                    )
            except Exception as e:
                self.logger.error(
                    f"Error flushing buffered fetches for {key}: {str(e)}",
                    exc_info=True,
                )

        if self._manager is not None:
            self._manager.shutdown()
            self._manager = None
            self._accumulators = {}

    @staticmethod
    def _write_batch(
        storage: StorageInterface,
        batch: List[Tuple[datetime, str, bytes]],
        concatenate: bool,
        logger,
    ):
        """
        Write a batch of buffered fetches, all from the same window, to storage.

        @param storage: Storage interface to write to
        @param batch: (window, path, parquet bytes) tuples, oldest first
        @param concatenate: Write a single file instead of one per fetch
        @param logger: Logger to use
        """
        if not concatenate:
            for _, path, parquet_bytes in batch:
                storage.save_bytes(parquet_bytes, path)
            logger.info(f"Saved {len(batch)} buffered files, last one {batch[-1][1]}")
            return

        table = pa.concat_tables(
            [pq.read_table(BytesIO(parquet_bytes)) for _, _, parquet_bytes in batch],
            promote_options="default",
        )
        # Named after the first fetch: the aggregator groups files by that timestamp
        saved_path = storage.save_bytes(
            ParquetSerializer.pyarrow_table_to_bytes(table, compression="snappy"),
            batch[0][1],
        )
        logger.info(
            f"Saved {len(batch)} buffered fetches with {table.num_rows} records to {saved_path}"
        )

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
