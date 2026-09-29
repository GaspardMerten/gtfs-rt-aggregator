import hashlib
import json
import time
from datetime import datetime, timezone as dt_timezone
from io import BytesIO
import signal
from multiprocessing.managers import SyncManager
from typing import Dict, List, Any, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..aggregator.service import AggregatorService
from ..config.models import ApiConfig, GtfsRtConfig, ProviderConfig
from ..fetcher.filter import build_filter
from ..fetcher.gtfs_rt import GtfsRtFetcher, row_metadata
from ..static.service import manifest_tables, read_latest, static_base
from ..storage.base import StorageInterface
from ..utils.log_helper import setup_logger
from ..utils.redact import redact
from ..utils.file_time import format_file_time
from ..utils.serializer import ParquetSerializer

# How long a job reuses the static version found by a previous job
STATIC_VERSION_TTL_SECONDS = 60


def _manager_init():
    """
    Runs in the Manager process. systemd sends SIGTERM to every process of the
    service at once: the Manager must outlive the main process's final flush,
    which shuts it down. It still ends if the main process dies (Linux).
    """
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        import ctypes

        PR_SET_PDEATHSIG = 1
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
    except (OSError, AttributeError):
        pass


def _start_manager() -> SyncManager:
    manager = SyncManager()
    manager.start(_manager_init)
    return manager


def feed_slug(api: ApiConfig) -> str:
    """Short stable name of a realtime feed, for its status file."""
    url_hash = hashlib.sha1(api.url.encode()).hexdigest()[:8]
    return f"{'-'.join(api.services)}-{url_hash}"


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

        # Fetch jobs run in separate processes, so what they share (the previous
        # fetch of each feed, accumulated fetches) lives in a Manager process.
        # Proxies are created up front so the jobs never need the Manager itself.
        self._manager = None
        self._feeds = {}
        self._accumulators = {}
        for provider in config.providers:
            for api in provider.realtime:
                if self._manager is None:
                    self._manager = _start_manager()
                self._feeds[self._feed_key(provider.name, api.url)] = {
                    "lock": self._manager.Lock(),
                    "state": self._manager.dict(),
                }
                if not api.accumulate_minutes:
                    continue
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
    def _feed_key(provider_name: str, url: str) -> str:
        return f"{provider_name}|{url}"

    @staticmethod
    def _accumulator_key(provider_name: str, url: str, service_type: str) -> str:
        return f"{provider_name}|{url}|{service_type}"

    def _find(self, provider_name: str, url: str) -> Tuple[ProviderConfig, ApiConfig]:
        provider = next(p for p in self.config.providers if p.name == provider_name)
        api = next(a for a in provider.realtime if a.url == url)
        return provider, api

    def get_scheduling(self) -> List[Tuple[Any, callable, str, Dict[str, Any]]]:
        """
        Get the scheduling configuration for the fetcher service.

        Returns:
            List of tuples containing (schedule job, function, arguments)
        """
        schedules = []
        for provider in self.config.providers:
            for api in provider.realtime:
                args = {
                    "provider_name": provider.name,
                    "url": api.url,
                    "service_types": api.services,
                    "timezone": provider.timezone,
                    "headers": api.headers,
                }
                name = f"Fetcher - {provider.name} - {feed_slug(api)}"
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
        provider, api = self._find(provider_name, url)
        feed = self._feeds[self._feed_key(provider_name, url)]
        storage = self._get_storage_for_provider(provider_name)
        fetch_time = datetime.now(pytz.timezone(timezone))
        status = {"last_attempt": fetch_time.isoformat()}

        try:
            data = GtfsRtFetcher.fetch_feed(url, headers, api.retries)
            message = GtfsRtFetcher.parse_message(data)
            header_timestamp = message.header.timestamp or None
            static_version, tables = self._static_version(provider, api, storage, feed)

            entities = list(message.entity)
            if api.filter:
                entity_filter = build_filter(
                    api.filter,
                    storage,
                    tables,
                    f"{provider_name}|{api.static}|{static_version}",
                )
                if entity_filter is None:
                    job_logger.warning(
                        f"{url}: no static version stored yet, filter not applied"
                    )
                else:
                    entities = [e for e in entities if entity_filter.keep(e)]

            # Same entities as the previous fetch (in any order): nothing new
            snapshot = hashlib.blake2b(
                "".join(
                    sorted(GtfsRtFetcher.entity_hash(e) for e in entities)
                ).encode(),
                digest_size=16,
            ).hexdigest()
            with feed["lock"]:
                unchanged = feed["state"].get("snapshot") == snapshot

            status.update(
                last_success=fetch_time.isoformat(),
                feed_timestamp=header_timestamp,
                feed_age_seconds=(
                    round(fetch_time.timestamp() - header_timestamp)
                    if header_timestamp
                    else None
                ),
                entity_count=len(message.entity),
                kept_count=len(entities),
                unchanged=unchanged,
                static_version=static_version,
            )
            if unchanged and api.skip_unchanged:
                job_logger.info(
                    f"{url}: unchanged since the previous fetch, not stored"
                )
                # Nothing new arrives to write the window that just ended
                self._flush_ended_windows(
                    provider_name, url, fetch_time, storage, job_logger
                )
                return

            result = GtfsRtFetcher.to_tables(
                GtfsRtFetcher.entities_by_service(entities),
                service_types,
                fetch_time,
                row_metadata(
                    provider_name, fetch_time, header_timestamp, static_version
                ),
            )
            self._store(provider_name, url, result, fetch_time, storage, job_logger)
            # Only once stored: after a failed save, the same content is tried again
            with feed["lock"]:
                feed["state"]["snapshot"] = snapshot
        except Exception as e:
            status.update(
                last_error=redact(str(e)), last_error_at=fetch_time.isoformat()
            )
            job_logger.error(f"Error in fetch job for {url}: {str(e)}", exc_info=True)
        finally:
            self._write_status(provider_name, api, feed, status, storage, job_logger)

    def _static_version(
        self, provider: ProviderConfig, api: ApiConfig, storage, feed
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        """
        Current version of the provider's static feed and the path of its
        tables, or (None, None). Looked up at most once a minute across jobs.
        """
        static = provider.static_for(api)
        if static is None:
            return None, None
        state = feed["state"]
        if time.time() - state.get("static_checked_at", 0) < STATIC_VERSION_TTL_SECONDS:
            return state.get("static_version"), state.get("static_tables")

        base = static_base(provider.name, static.name)
        try:
            latest = read_latest(
                self.storages.get(provider.name, self.storages["global"]),
                base,
                self.logger,
            )
        except Exception as e:
            # A storage error must not fail the fetch: keep the last known version
            self.logger.warning(f"Could not read the static version of {base}: {e}")
            return state.get("static_version"), state.get("static_tables")
        version = latest.get("version") if latest else None
        tables = manifest_tables(latest, base) if latest else None
        state.update(
            static_checked_at=time.time(), static_version=version, static_tables=tables
        )
        return version, tables

    def _write_status(self, provider_name, api, feed, status, storage, logger):
        """
        Write the feed's status.json: last attempt, success and error, feed age,
        entity counts. Keeps the fields of earlier fetches (e.g. the last success
        after a failure).
        """
        try:
            with feed["lock"]:
                merged = dict(feed["state"].get("status", {}))
                merged.update(status)
                feed["state"]["status"] = merged
            # The query string may hold an API key
            merged = {"url": api.url.split("?")[0], "services": api.services, **merged}
            storage.save_bytes(
                json.dumps(merged, indent=2).encode("utf-8"),
                f"{provider_name}/_status/{feed_slug(api)}.json",
            )
        except Exception as e:
            logger.warning(f"Could not write the status of {api.url}: {e}")

    def _flush_ended_windows(self, provider_name, url, fetch_time, storage, job_logger):
        """Write accumulated windows of this feed that ended before fetch_time."""
        for key, accumulator in self._accumulators.items():
            if not key.startswith(self._feed_key(provider_name, url) + "|"):
                continue
            window = AggregatorService._get_rounded_time(
                fetch_time, accumulator["minutes"]
            )
            with accumulator["lock"]:
                buffer = accumulator["buffer"]
                if not len(buffer) or buffer[0][0] >= window:
                    continue
                batch = buffer[:]
                del buffer[:]
            self._write_batch(storage, batch, accumulator["concatenate"], job_logger)

    def _store(self, provider_name, url, result, fetch_time, storage, job_logger):
        """Save each service table, or buffer it if the feed accumulates."""
        for service_type, df in result.items():
            parquet_bytes = ParquetSerializer.pyarrow_table_to_bytes(
                df, compression="snappy"
            )
            filename = f"individual/{format_file_time(fetch_time)}.parquet"
            path = f"{provider_name}/{service_type}/{filename}"

            accumulator = self._accumulators.get(
                self._accumulator_key(provider_name, url, service_type)
            )

            if accumulator is None:
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
