"""Processing of one realtime fetch, shared by the worker processes and FetcherService (a helper to fetch from your own code)."""

import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pyarrow as pa

from ..config.models import ApiConfig, GtfsRtConfig, ProviderConfig
from ..fetcher.filter import build_filter
from ..fetcher.gtfs_rt import GtfsRtFetcher, row_metadata
from ..static.service import manifest_tables, read_latest, static_base
from ..storage.base import StorageInterface, storage_for
from ..utils.redact import strip_query

# How long the static version found for a provider is reused; when none is
# stored yet (first start), it is looked for again sooner
STATIC_VERSION_TTL_SECONDS = 60


class StaticNotReady(Exception):
    """
    A feed's filter needs the static version, which cannot be read yet (not
    stored yet after the first start, or storage unavailable). The fetch
    must wait: stored unfiltered, it would archive the rows the filter drops.
    """


MISSING_STATIC_VERSION_TTL_SECONDS = 5


def feed_slug(api: ApiConfig) -> str:
    """Short stable name of a realtime feed, for its status file."""
    return f"{'-'.join(api.services)}-{feed_hash(api)}"


def feed_hash(api: ApiConfig) -> str:
    """8 hex characters identifying a realtime feed (feedId column, file names)."""
    return hashlib.sha1(api.url.encode()).hexdigest()[:8]


def feed_id(provider_name: str, api: ApiConfig) -> str:
    """Name of a realtime feed in the spool, unique across providers."""
    return f"{provider_name}__{feed_slug(api)}"


def realtime_feeds(config: GtfsRtConfig) -> Dict[str, Tuple[ProviderConfig, ApiConfig]]:
    """Every realtime feed of the configuration, by feed id."""
    return {
        feed_id(provider.name, api): (provider, api)
        for provider in config.providers
        for api in provider.realtime
    }


class StaticVersions:
    """
    Current static version of each provider, read from latest.json at most
    once a minute per process. A storage error keeps the last known version.
    """

    def __init__(self, storages: Dict[str, StorageInterface], logger=None):
        self.storages = storages
        self.logger = logger or logging.getLogger(__name__)
        self._cache: Dict[
            str, Tuple[float, Optional[str], Optional[Dict[str, str]]]
        ] = {}

    def get(
        self, provider: ProviderConfig, api: ApiConfig
    ) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
        static = provider.static_for(api)
        if static is None:
            return None, None
        base = static_base(provider.name, static.name)
        cached = self._cache.get(base)
        if cached:
            ttl = (
                STATIC_VERSION_TTL_SECONDS
                if cached[1]
                else MISSING_STATIC_VERSION_TTL_SECONDS
            )
            if time.time() - cached[0] < ttl:
                return cached[1], cached[2]
        try:
            latest = read_latest(
                storage_for(self.storages, provider.name),
                base,
                self.logger,
            )
        except Exception as e:
            self.logger.warning(f"Could not read the static version of {base}: {e}")
            return (cached[1], cached[2]) if cached else (None, None)
        version = latest.get("version") if latest else None
        tables = manifest_tables(latest, base) if latest else None
        self._cache[base] = (time.time(), version, tables)
        return version, tables


@dataclass
class ProcessResult:
    """Outcome of processing one fetch."""

    snapshot: str
    unchanged: bool
    # Tables to store by service type; None when skipped as unchanged
    tables: Optional[Dict[str, pa.Table]]
    summary: Dict = field(default_factory=dict)


def snapshot_of(entities: List, hashes: Optional[List[str]] = None) -> str:
    """Hash of a set of entities, whatever their order."""
    if hashes is None:
        hashes = [GtfsRtFetcher.entity_hash(e) for e in entities]
    return hashlib.blake2b("".join(sorted(hashes)).encode(), digest_size=16).hexdigest()


def process_payload(
    data: bytes,
    fetch_time: datetime,
    provider: ProviderConfig,
    api: ApiConfig,
    storage: StorageInterface,
    static_versions: StaticVersions,
    previous_snapshot: Optional[str],
    logger=None,
) -> ProcessResult:
    """
    Parse a fetched GTFS-RT feed, filter it and build its tables.

    @param data: The feed, as downloaded
    @param fetch_time: When it was fetched, aware, in the provider timezone
    @param provider: Provider of the feed
    @param api: The realtime feed
    @param storage: Storage of the provider (for the static version)
    @param static_versions: Static version lookup
    @param previous_snapshot: Snapshot of the previous stored fetch of the feed
    @return Tables to store, or none if nothing changed since the previous fetch
    """
    logger = logger or logging.getLogger(__name__)
    message = GtfsRtFetcher.parse_message(data)
    header_timestamp = message.header.timestamp or None
    static_version, tables = static_versions.get(provider, api)

    entities = list(message.entity)
    if api.filter:
        entity_filter = build_filter(
            api.filter,
            storage,
            tables,
            f"{provider.name}|{api.static}|{static_version}",
        )
        if entity_filter is None:
            raise StaticNotReady(
                f"{provider.name}: the filter of {strip_query(api.url)} needs a "
                "static version, none can be read yet"
            )
        entities = [e for e in entities if entity_filter.keep(e)]

    # Same entities as the previous fetch (in any order): nothing new
    hashes = [GtfsRtFetcher.entity_hash(e) for e in entities]
    snapshot = snapshot_of(entities, hashes)
    unchanged = snapshot == previous_snapshot
    summary = {
        "feed_timestamp": header_timestamp,
        "feed_age_seconds": (
            round(fetch_time.timestamp() - header_timestamp)
            if header_timestamp
            else None
        ),
        "entity_count": len(message.entity),
        "kept_count": len(entities),
        "unchanged": unchanged,
        "static_version": static_version,
    }
    if unchanged and api.skip_unchanged:
        return ProcessResult(snapshot, True, None, summary)

    result = GtfsRtFetcher.build_tables(
        entities,
        hashes,
        api.services,
        fetch_time,
        row_metadata(
            provider.name,
            fetch_time,
            header_timestamp,
            static_version,
            feed_hash(api),
        ),
    )
    return ProcessResult(snapshot, unchanged, result, summary)
