import os
from typing import List, Optional, Dict, Any, Union

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class StorageConfig(BaseModel):
    """Storage configuration."""

    type: str = Field(..., description="Storage type ('filesystem' or 'gcs')")
    params: Dict[str, Any] = Field(
        default_factory=dict, description="Storage-specific parameters"
    )

    @field_validator("type")
    @classmethod
    def validate_storage_type(cls, v: str) -> str:
        # Unknown types are rejected by StorageFactory, which also accepts
        # types registered at runtime
        return v.lower()

    @model_validator(mode="after")
    def validate_params(self):
        required = {
            "gcs": ["bucket_name"],
            "google": ["bucket_name"],
            "google_cloud_storage": ["bucket_name"],
            "minio": ["endpoint", "access_key", "secret_key", "bucket_name"],
            "s3": ["endpoint", "access_key", "secret_key", "bucket_name"],
        }.get(self.type, [])
        missing = [name for name in required if name not in self.params]
        if missing:
            raise ValueError(
                f"Storage type {self.type} needs params: {', '.join(missing)}"
            )
        return self


class FilterConfig(BaseModel):
    """Rows of a realtime feed to keep. A row is kept if it matches any rule."""

    model_config = ConfigDict(extra="forbid")

    route_types: List[Union[int, str]] = Field(
        default_factory=list,
        description='GTFS route types to keep, e.g. [2, "100-199"] for rail. Needs a static feed.',
    )
    route_ids: List[str] = Field(default_factory=list, description="Route ids to keep")
    trip_ids: List[str] = Field(default_factory=list, description="Trip ids to keep")
    keep_unmatched_added: bool = Field(
        False,
        description="Keep ADDED, NEW and DUPLICATED trips whose route cannot be resolved through the static feed",
    )

    @field_validator("route_types", mode="before")
    @classmethod
    def validate_route_types(cls, v):
        for item in v:
            if isinstance(item, bool) or not isinstance(item, (int, str)):
                raise ValueError(
                    f"route_types entries are numbers or ranges, got {item!r}"
                )
            if isinstance(item, str):
                low, _, high = item.partition("-")
                if not (low.strip().isdigit() and high.strip().isdigit()):
                    raise ValueError(
                        f'route_types entries are numbers or ranges like "100-199", got {item!r}'
                    )
                if int(low) > int(high):
                    raise ValueError(f"Empty route_types range {item!r}")
        return v

    def route_type_set(self) -> set:
        types = set()
        for item in self.route_types:
            if isinstance(item, int):
                types.add(item)
            else:
                low, _, high = item.partition("-")
                types.update(range(int(low), int(high) + 1))
        return types

    @model_validator(mode="after")
    def validate_keep_unmatched_added(self):
        if self.keep_unmatched_added and not (self.route_types or self.route_ids):
            raise ValueError(
                "keep_unmatched_added only applies with route_types or route_ids "
                "(it keeps added trips whose route cannot be resolved)"
            )
        return self

    @property
    def needs_static(self) -> bool:
        # Route ids and types are matched through the static trips and routes
        return bool(self.route_types or self.route_ids)


class ApiConfig(BaseModel):
    """GTFS-RT (realtime) feed configuration for a provider."""

    # Catch typos such as refresh_second instead of silently using the default
    model_config = ConfigDict(extra="forbid")

    url: str = Field(..., description="URL of the GTFS-RT feed")
    services: List[str] = Field(
        ...,
        description="List of service types to fetch (VehiclePosition, TripUpdate, Alert, TripModifications)",
    )
    refresh_seconds: int = Field(
        60, gt=0, description="How often to fetch data from this API (in seconds)"
    )
    frequency_minutes: int = Field(
        60, gt=0, description="How often to group data (in minutes)"
    )
    check_interval_seconds: int = Field(
        300,
        gt=0,
        description="How often to check for new files to aggregate (in seconds)",
    )
    accumulate_minutes: int = Field(
        0,
        ge=0,
        description="Keep fetches in memory and write them in clock-aligned blocks of this many minutes (0 writes every fetch right away)",
    )
    accumulate_concatenate: bool = Field(
        True,
        description="Write each block as one Parquet file instead of one file per fetch",
    )
    headers: Dict[str, str] = Field(
        default_factory=dict,
        description="HTTP headers sent with each request (e.g. an API key)",
    )
    retries: int = Field(
        3, ge=0, description="Retries on connection errors, timeouts and 429/5xx"
    )
    skip_unchanged: bool = Field(
        True,
        description="Do not store a fetch whose entities are the same as the previous one",
    )
    deduplicate: bool = Field(
        False,
        description="When aggregating, merge consecutive identical rows of an entity into one row with firstSeen and lastSeen",
    )
    static: Optional[str] = Field(
        None,
        description="Name of the provider's static feed used for staticVersion and the filter (needed only if it has several)",
    )
    filter: Optional[FilterConfig] = Field(
        None, description="Rows to keep; all rows are kept if not set"
    )
    priority: int = Field(
        0,
        description="When the spool is full, feeds with the lowest priority stop being fetched first",
    )

    @model_validator(mode="after")
    def validate_accumulate_minutes(self):
        # Windows are aligned on minutes since midnight, and must never span two
        # aggregation periods
        if self.accumulate_minutes:
            if 1440 % self.accumulate_minutes:
                raise ValueError(
                    f"accumulate_minutes ({self.accumulate_minutes}) must divide a day (1440 minutes)"
                )
            if self.frequency_minutes % self.accumulate_minutes:
                raise ValueError(
                    f"accumulate_minutes ({self.accumulate_minutes}) must divide frequency_minutes ({self.frequency_minutes})"
                )
        return self

    @field_validator("services")
    @classmethod
    def validate_services(cls, v: List[str]) -> List[str]:
        valid_services = {"VehiclePosition", "TripUpdate", "Alert", "TripModifications"}
        if not v:
            raise ValueError("services cannot be empty")
        invalid = [service for service in v if service not in valid_services]
        if invalid:
            raise ValueError(
                f"Invalid service type: {', '.join(invalid)}. Must be one of: {', '.join(sorted(valid_services))}"
            )
        return v


class StaticConfig(BaseModel):
    """GTFS static feed configuration for a provider."""

    # Catch typos such as check_minute instead of silently using the default
    model_config = ConfigDict(extra="forbid")

    url: Optional[str] = Field(None, description="URL of the GTFS zip")
    index_url: Optional[str] = Field(
        None,
        description="Page listing the GTFS zip, for feeds whose URL changes (with url_pattern)",
    )
    url_pattern: Optional[str] = Field(
        None,
        description="Regular expression matching the zip links on index_url; the greatest match is used",
    )
    name: str = Field(
        "static",
        # A single folder name, not clashing with _status
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
        description="Folder the versions are stored in, under the provider folder",
    )
    check_minutes: int = Field(
        60, gt=0, description="How often to check for a new version (in minutes)"
    )
    headers: Dict[str, str] = Field(
        default_factory=dict,
        description="HTTP headers sent with each request (e.g. an API key)",
    )
    retries: int = Field(
        3, ge=0, description="Retries on connection errors, timeouts and 429/5xx"
    )
    reuse_unchanged_tables: bool = Field(
        False,
        description="Point to the previous version's file for tables whose source file did not change, instead of storing them again",
    )

    @model_validator(mode="after")
    def validate_source(self):
        if bool(self.url) == bool(self.index_url):
            raise ValueError(
                "A static feed needs either url, or index_url and url_pattern"
            )
        if self.index_url and not self.url_pattern:
            raise ValueError("index_url needs a url_pattern")
        if self.url and self.url_pattern:
            raise ValueError("url_pattern is only used with index_url")
        if self.url_pattern:
            import re

            try:
                re.compile(self.url_pattern)
            except re.error as e:
                raise ValueError(f"Invalid url_pattern: {e}")
        return self


class ProviderConfig(BaseModel):
    """Provider configuration."""

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(..., description="Name of the provider")
    timezone: str = Field("UTC", description="Timezone of the provider")
    realtime: List[ApiConfig] = Field(
        default_factory=list,
        # "apis" is the name used before 0.3.0
        validation_alias=AliasChoices("realtime", "apis"),
        description="GTFS-RT feeds for this provider",
    )
    static: List[StaticConfig] = Field(
        default_factory=list, description="GTFS static feeds for this provider"
    )
    frequency_minutes: Optional[int] = Field(
        None, gt=0, description="Default grouping frequency for all APIs (in minutes)"
    )
    check_interval_seconds: Optional[int] = Field(
        None, gt=0, description="Default check interval for all APIs (in seconds)"
    )
    storage: Optional[StorageConfig] = Field(
        None, description="Provider-specific storage configuration (overrides global)"
    )

    @model_validator(mode="after")
    def validate_feeds(self):
        if not self.realtime and not self.static:
            raise ValueError(
                f"Provider {self.name} has no realtime or static feed defined"
            )
        names = [feed.name for feed in self.static]
        if len(names) != len(set(names)):
            raise ValueError(
                f"Static feeds of provider {self.name} need distinct names, got {names}"
            )
        services = {service for api in self.realtime for service in api.services}
        if services & set(names):
            raise ValueError(
                f"Static feed names of provider {self.name} cannot be a realtime service type: {sorted(services & set(names))}"
            )
        for api in self.realtime:
            if api.static is not None and api.static not in names:
                raise ValueError(
                    f"Realtime feed {api.url} of provider {self.name} refers to static feed {api.static!r}, which is not defined"
                )
            if api.filter and api.filter.needs_static and self.static_for(api) is None:
                raise ValueError(
                    f"The route filter of {api.url} needs a static feed in provider {self.name}"
                    + (
                        ' (several are defined: set static = "<name>")'
                        if self.static
                        else ""
                    )
                )
        return self

    def static_for(self, api: "ApiConfig") -> Optional["StaticConfig"]:
        """Static feed matching a realtime feed: the one it names, or the only one."""
        if api.static is not None:
            return next(feed for feed in self.static if feed.name == api.static)
        return self.static[0] if len(self.static) == 1 else None

    @property
    def apis(self) -> List[ApiConfig]:
        """Realtime feeds, under the name used before 0.3.0."""
        return self.realtime

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        import pytz

        try:
            pytz.timezone(v)
        except pytz.UnknownTimeZoneError:
            raise ValueError(f"Invalid timezone: {v}")
        return v


DEFAULT_PATH_TEMPLATE = (
    "provider={provider}/service={service}/date={start:%Y-%m-%d}/"
    "{start:%H-%M-%S}_to_{end:%H-%M-%S}.parquet"
)


class OutputConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path_template: str = Field(
        DEFAULT_PATH_TEMPLATE,
        description="Path of aggregated files. Fields: provider, service, start, end (datetimes in the provider timezone)",
    )
    compact_daily: bool = Field(
        False,
        description="Once a day is over, merge its aggregated files into one file sorted by sort_by",
    )
    compacted_name: str = Field(
        "day.parquet", description="File name of a compacted day, in the day's folder"
    )
    sort_by: List[str] = Field(
        default_factory=lambda: ["entityId", "fetchTime"],
        description="Columns a compacted day is sorted by",
    )

    @field_validator("path_template")
    @classmethod
    def validate_path_template(cls, v: str) -> str:
        from datetime import datetime

        try:
            path = v.format(
                provider="p",
                service="s",
                start=datetime(2026, 1, 1),
                end=datetime(2026, 1, 1, 1),
            )
        except (KeyError, IndexError, ValueError) as e:
            raise ValueError(f"Invalid path_template {v!r}: {e}")
        for field in ("{provider}", "{service}", "{start"):
            if field not in v:
                raise ValueError(
                    f"path_template must contain {field}, or different data would share a file"
                )
        if ".." in path.split("/"):
            raise ValueError("path_template cannot contain ..")
        if path.startswith("/") or not path.endswith(".parquet"):
            raise ValueError("path_template must be a relative path ending in .parquet")
        return v

    @model_validator(mode="after")
    def validate_compaction(self):
        if self.compact_daily:
            from datetime import datetime
            import posixpath

            def folder(start):
                return posixpath.dirname(
                    self.path_template.format(
                        provider="p", service="s", start=start, end=start
                    )
                )

            # One folder per day, holding the whole day
            first = folder(datetime(2026, 1, 1))
            if first == folder(datetime(2026, 1, 2)) or first != folder(
                datetime(2026, 1, 1, 23, 59)
            ):
                raise ValueError(
                    "compact_daily needs a path_template with one folder per day (e.g. date={start:%Y-%m-%d}/)"
                )
        return self


class RuntimeConfig(BaseModel):
    """How the pipeline runs: spool on disk, threads and worker processes."""

    model_config = ConfigDict(extra="forbid")

    spool_dir: Optional[str] = Field(
        None,
        description="Folder of downloads and results waiting to be processed or uploaded (default: <TMPDIR>/gtfs_rt_aggregator-spool)",
    )
    spool_max_gb: float = Field(
        10,
        gt=0,
        description="Past this size, the lowest-priority feeds stop being fetched",
    )
    fetch_threads: int = Field(
        16, ge=1, description="Downloads running at the same time"
    )
    workers: Union[int, str] = Field(
        "auto", description='Worker processes; "auto": number of CPUs - 1, at least 1'
    )
    heavy_slots: int = Field(
        1,
        ge=1,
        description="Worker processes for memory-heavy work (large fetches, static feeds, aggregation, compaction)",
    )
    heavy_threshold_mb: float = Field(
        8, gt=0, description="Fetches larger than this go to the heavy workers"
    )
    max_attempts: int = Field(
        3, ge=1, description="Tries per fetch before it is moved to quarantine/"
    )
    startup_jitter_seconds: float = Field(
        60,
        ge=0,
        description="Jobs start at a random time within this delay (or their interval), not all at once",
    )
    max_tasks_per_worker: int = Field(
        200, ge=1, description="A worker process is replaced after this many tasks"
    )

    @field_validator("workers")
    @classmethod
    def validate_workers(cls, v):
        if v == "auto" or (isinstance(v, int) and not isinstance(v, bool) and v >= 1):
            return v
        raise ValueError(f'workers must be "auto" or a positive number, got {v!r}')

    def worker_count(self) -> int:
        if self.workers == "auto":
            from ..utils.cpu import available_cpus

            # CPUs minus one for the main process, the fetch and upload threads
            return max(1, available_cpus() - 1)
        return self.workers


class RawConfig(BaseModel):
    """Optional archive of the raw GTFS-RT fetches."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        False, description="Keep every fetch, bundled per feed and hour"
    )
    prefix: str = Field(
        "raw",
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._/-]*$",
        description="Folder of the archive under each provider's storage root",
    )


class GtfsRtConfig(BaseModel):
    """Main configuration for the GTFS-RT fetcher and aggregator."""

    storage: StorageConfig = Field(..., description="Global storage configuration")
    providers: List[ProviderConfig] = Field(..., description="List of providers")
    runtime: RuntimeConfig = Field(
        default_factory=RuntimeConfig, description="How the pipeline runs"
    )
    raw: RawConfig = Field(default_factory=RawConfig, description="Raw archive")
    output: OutputConfig = Field(
        default_factory=OutputConfig, description="Output configuration"
    )

    @field_validator("providers")
    @classmethod
    def validate_provider_names(
        cls, providers: List[ProviderConfig]
    ) -> List[ProviderConfig]:
        names = [p.name for p in providers]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"Provider names must be unique: {', '.join(duplicates)}")
        return providers

    def get_provider_storage(self, provider_name: str) -> StorageConfig:
        """
        Get storage configuration for a provider.

        @param provider_name: Name of the provider
        @return Storage configuration for the provider
        @raises ValueError: If provider is not found
        """
        # Find the provider
        provider = None
        for p in self.providers:
            if p.name == provider_name:
                provider = p
                break

        if not provider:
            raise ValueError(f"Provider not found: {provider_name}")

        # Use provider-specific storage if available, otherwise use global
        return provider.storage or self.storage

    def get_effective_api_config(self, provider_name: str, api_url: str) -> ApiConfig:
        """
        Get effective API configuration for a provider and URL.

        @param provider_name: Name of the provider
        @param api_url: URL of the API
        @return Effective API configuration
        @raises ValueError: If provider or API is not found
        """
        # Find the provider
        provider = None
        for p in self.providers:
            if p.name == provider_name:
                provider = p
                break

        if not provider:
            raise ValueError(f"Provider not found: {provider_name}")

        # Find the API
        api = None
        for a in provider.realtime:
            if a.url == api_url:
                api = a
                break

        if not api:
            raise ValueError(f"API not found: {api_url}")

        # Apply provider defaults if needed
        if provider.frequency_minutes is not None and api.frequency_minutes == 60:
            api.frequency_minutes = provider.frequency_minutes

        if (
            provider.check_interval_seconds is not None
            and api.check_interval_seconds == 300
        ):
            api.check_interval_seconds = provider.check_interval_seconds

        return api
