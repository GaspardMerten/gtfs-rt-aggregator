import json
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import pytz

from ..config.models import ApiConfig, GtfsRtConfig, ProviderConfig
from ..fetcher.gtfs_rt import GtfsRtFetcher
from ..runtime.core import StaticVersions, feed_slug, process_payload
from ..storage.base import StorageInterface
from ..utils.file_time import format_file_time
from ..utils.log_helper import setup_logger
from ..utils.redact import redact
from ..utils.serializer import ParquetSerializer

__all__ = ["FetcherService", "feed_slug"]


class FetcherService:
    """
    Fetches GTFS-RT feeds and stores each fetch, in the calling process.

    The pipeline itself uses the disk-spool runtime (gtfs_rt_aggregator.runtime),
    which also handles accumulate_minutes. This class is a simple way to fetch
    feeds from your own code: each run_once stores one file per service type.
    """

    def __init__(self, config: GtfsRtConfig, storages: Dict[str, StorageInterface]):
        """
        Initialize the fetcher service.

        @param config: Configuration
        @param storages: Dictionary of storage interfaces by provider name, with 'global' as the default
        """
        self.logger = setup_logger(f"{__name__}.FetcherService")
        self.config = config
        self.storages = storages
        self._static_versions = StaticVersions(storages, self.logger)
        # Per feed: snapshot of the previous stored fetch, merged status
        self._state: Dict[str, Dict[str, Any]] = {}

    def _find(self, provider_name: str, url: str) -> Tuple[ProviderConfig, ApiConfig]:
        provider = next(p for p in self.config.providers if p.name == provider_name)
        api = next(a for a in provider.realtime if a.url == url)
        return provider, api

    def get_scheduling(self) -> List[Tuple[Any, callable, str, Dict[str, Any]]]:
        """
        Get the scheduling configuration for the fetcher service.

        Returns:
            List of tuples containing (interval in seconds, function, name, arguments)
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
        Fetch a feed once and store it (one file per service type).

        @param provider_name: Name of the provider
        @param url: URL of the GTFS-RT feed
        @param service_types: List of service types to fetch
        @param timezone: Timezone of the provider
        @param headers: HTTP headers to send (e.g. an API key)
        """
        job_logger = setup_logger(f"{__name__}.FetcherService.job.{provider_name}")
        provider, api = self._find(provider_name, url)
        state = self._state.setdefault(f"{provider_name}|{url}", {})
        storage = self._get_storage_for_provider(provider_name)
        fetch_time = datetime.now(pytz.timezone(timezone))
        status = {"last_attempt": fetch_time.isoformat()}

        try:
            data = GtfsRtFetcher.fetch_feed(url, headers, api.retries)
            result = process_payload(
                data,
                fetch_time,
                provider,
                api,
                storage,
                self._static_versions,
                state.get("snapshot"),
                job_logger,
            )
            status.update(last_success=fetch_time.isoformat(), **result.summary)
            if result.tables is None:
                job_logger.info(
                    f"{url}: unchanged since the previous fetch, not stored"
                )
                return
            for service_type, table in result.tables.items():
                if service_type not in service_types:
                    continue
                path = f"{provider_name}/{service_type}/individual/{format_file_time(fetch_time)}.parquet"
                storage.save_bytes(
                    ParquetSerializer.pyarrow_table_to_bytes(
                        table, compression="snappy"
                    ),
                    path,
                )
                job_logger.info(
                    f"Saved {table.num_rows} {service_type} records to {path}"
                )
            # Only once stored: after a failed save, the same content is tried again
            state["snapshot"] = result.snapshot
        except Exception as e:
            status.update(
                last_error=redact(str(e)), last_error_at=fetch_time.isoformat()
            )
            job_logger.error(f"Error in fetch job for {url}: {str(e)}", exc_info=True)
        finally:
            self._write_status(provider_name, api, state, status, storage, job_logger)

    def flush_all(self):
        """Nothing to flush: kept for compatibility (accumulation is done by the runtime)."""

    def _write_status(self, provider_name, api, state, status, storage, logger):
        """Write the feed's status.json, keeping the fields of earlier fetches."""
        try:
            merged = {**state.get("status", {}), **status}
            state["status"] = merged
            document = {
                "url": api.url.split("?")[0],
                "services": api.services,
                **merged,
            }
            storage.save_bytes(
                json.dumps(document, indent=2).encode("utf-8"),
                f"{provider_name}/_status/{feed_slug(api)}.json",
            )
        except Exception as e:
            logger.warning(f"Could not write the status of {redact(api.url)}: {e}")

    def _get_storage_for_provider(self, provider_name: str) -> StorageInterface:
        """
        Get the storage interface for a provider.

        @param provider_name: Name of the provider
        @return Storage interface for the provider
        """
        return self.storages.get(provider_name, self.storages["global"])
