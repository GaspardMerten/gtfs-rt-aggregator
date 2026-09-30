from typing import List, Optional, Dict, Any, Union

from ..utils.redact import strip_query
from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


def _check_source(url, adapter, what: str):
    if bool(url) == bool(adapter):
        raise ValueError(f"{what} needs either url or adapter")
    if adapter:
        from ..adapters import parse_spec

        parse_spec(adapter)


class _Model(BaseModel):
    # Validation errors would print the values, which can be secrets (API
    # keys in URLs and headers, storage credentials)
    model_config = ConfigDict(hide_input_in_errors=True)


class StorageConfig(_Model):
    """Storage configuration."""

    type: str = Field(
        ..., description="Storage type: filesystem, gcs, or minio (also s3)"
    )
    params: Dict[str, Any] = Field(
        default_factory=dict, description="Storage-specific parameters"
    )

    @field_validator("type")
    @classmethod
    def validate_storage_type(cls, v: str) -> str:
        known = ("filesystem", "gcs", "google", "google_cloud_storage", "minio", "s3")
        if v.lower() not in known:
            raise ValueError(
                f"Unsupported storage type {v!r}: use filesystem, gcs or minio"
            )
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


class FilterConfig(_Model):
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


class ApiConfig(_Model):
    """GTFS-RT (realtime) feed configuration for a provider."""

    # Catch typos such as refresh_second instead of silently using the default
    model_config = ConfigDict(extra="forbid")

    url: Optional[str] = Field(None, description="URL of the GTFS-RT feed")
    adapter: Optional[str] = Field(
        None,
        description="Instead of url: Python function giving each fetch, 'path/to/file.py:function' (relative to the config file) or 'module:function' (see gtfs_rt_aggregator.adapters)",
    )
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
        description="Collect fetches on disk and store them as one file per clock-aligned block of this many minutes (0 stores every fetch right away)",
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

    @model_validator(mode="before")
    @classmethod
    def drop_removed_options(cls, values):
        if isinstance(values, dict) and "accumulate_concatenate" in values:
            import logging

            values = dict(values)
            values.pop("accumulate_concatenate")
            # Logged: a DeprecationWarning raised here is hidden by default
            logging.getLogger(__name__).warning(
                "accumulate_concatenate is ignored since 0.7.4 (a window is always one file): remove it"
            )
        return values

    @model_validator(mode="after")
    def validate_source(self):
        _check_source(self.url, self.adapter, "A realtime feed")
        return self

    @property
    def source(self) -> str:
        """The feed's URL (without query string, which may hold a key) or adapter."""
        return strip_query(self.url) if self.url else f"adapter {self.adapter}"

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


class StaticConfig(_Model):
    """GTFS static feed configuration for a provider."""

    # Catch typos such as check_minute instead of silently using the default
    model_config = ConfigDict(extra="forbid")

    url: Optional[str] = Field(None, description="URL of the GTFS zip")
    adapter: Optional[str] = Field(
        None,
        description="Instead of url: Python function building the GTFS, 'path/to/file.py:function' or 'module:function' (see gtfs_rt_aggregator.adapters)",
    )
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
        if self.adapter is not None:
            if self.url or self.index_url or self.url_pattern:
                raise ValueError(
                    "A static feed needs one of url, index_url or adapter, not several"
                )
            _check_source(None, self.adapter, "A static feed")
            return self
        if bool(self.url) == bool(self.index_url):
            raise ValueError(
                "A static feed needs either url, index_url and url_pattern, or adapter"
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


class ProviderConfig(_Model):
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

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        # The name is a folder in the storage and in the spool, where "global"
        # and "__global__" hold files of the global storage
        if not v or v in ("global", "__global__", ".", "..") or "/" in v or "\\" in v:
            raise ValueError(
                f"Invalid provider name {v!r}: it must not be empty, global, __global__, . or .., or contain / or \\"
            )
        return v

    @model_validator(mode="after")
    def validate_feeds(self):
        # Provider defaults, for the feeds that do not set their own
        for api in self.realtime:
            for option in ("frequency_minutes", "check_interval_seconds"):
                value = getattr(self, option)
                if value is not None and option not in api.model_fields_set:
                    setattr(api, option, value)
            # Checked again with the provider's frequency_minutes
            api.validate_accumulate_minutes()
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
        urls = [api.url or api.adapter for api in self.realtime]
        if len(urls) != len(set(urls)):
            raise ValueError(f"Provider {self.name} lists the same realtime feed twice")
        # Feeds sharing a service write to the same files, aggregated together
        for service in services:
            feeds = [api for api in self.realtime if service in api.services]
            for option in ("frequency_minutes", "deduplicate"):
                if len({getattr(api, option) for api in feeds}) > 1:
                    raise ValueError(
                        f"The {service} feeds of provider {self.name} must have the same {option}"
                    )
        for api in self.realtime:
            if api.static is not None and api.static not in names:
                raise ValueError(
                    f"Realtime feed {api.source} of provider {self.name} refers to static feed {api.static!r}, which is not defined"
                )
            if api.filter and api.filter.needs_static and self.static_for(api) is None:
                raise ValueError(
                    f"The route filter of {api.source} needs a static feed in provider {self.name}"
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


class OutputConfig(_Model):
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
    trip_stop_events: bool = Field(
        False,
        description="Once a service date is over, write one row per trip and stop (service TripStopEvent), from the trip updates and the static timetable",
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
        if self.compact_daily or self.trip_stop_events:
            from datetime import datetime
            import posixpath

            def folder(start, service="s"):
                return posixpath.dirname(
                    self.path_template.format(
                        provider="p", service=service, start=start, end=start
                    )
                )

            # One folder per day, holding the whole day
            first = folder(datetime(2026, 1, 1))
            if first == folder(datetime(2026, 1, 2)) or first != folder(
                datetime(2026, 1, 1, 23, 59)
            ):
                raise ValueError(
                    "compact_daily and trip_stop_events need a path_template with one folder per day (e.g. date={start:%Y-%m-%d}/)"
                )
            if folder(datetime(2026, 1, 1)) == folder(datetime(2026, 1, 1), "t"):
                # The day files of all services would have the same path
                raise ValueError(
                    "compact_daily and trip_stop_events need {service} in the folders of path_template, not only in the file name"
                )
        return self


class RuntimeConfig(_Model):
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


class RawConfig(_Model):
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


class IcebergConfig(_Model):
    """Optional Iceberg tables over the compacted days (extra: iceberg)."""

    model_config = ConfigDict(extra="forbid")

    catalog: str = Field(
        "sql",
        pattern="^(sql|rest)$",
        description='"sql" (SQLite file, no service) or "rest" (Lakekeeper, Polaris, Nessie...)',
    )
    catalog_uri: str = Field(
        ..., description="sqlite:////path/catalog.db, or the REST catalog URL"
    )
    warehouse: str = Field(
        "iceberg",
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._/-]*$",
        description="Folder of the tables in the global storage",
    )
    namespace: str = Field("archive", pattern=r"^[A-Za-z0-9_]+$")
    services: List[str] = Field(
        default_factory=lambda: ["TripUpdate", "VehiclePosition"],
        description="One table per service type",
    )
    write_version_hint: bool = Field(
        True,
        description="Write metadata/version-hint.text, so readers without a catalog find the latest metadata",
    )
    expire_snapshots_days: int = Field(
        7, ge=1, description="Snapshots older than this are expired (weekly)"
    )
    sync_minutes: int = Field(
        60, gt=0, description="How often new compacted days are registered"
    )
    public_base_url: Optional[str] = Field(
        None,
        pattern=r"^https?://[^?#]+$",
        description="URL serving the storage's files by path (e.g. https://data.example.org): a copy of the metadata whose paths all use it is kept in public_warehouse",
    )
    public_warehouse: Optional[str] = Field(
        None,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._/-]*$",
        description="Folder of that public copy (default: <warehouse>-public)",
    )

    @model_validator(mode="after")
    def validate_public(self):
        if self.public_base_url is not None:
            self.public_base_url = self.public_base_url.rstrip("/")
            if self.public_warehouse is None:
                self.public_warehouse = f"{self.warehouse}-public"
            if self.public_warehouse == self.warehouse:
                raise ValueError("public_warehouse must differ from warehouse")
        return self

    @field_validator("services")
    @classmethod
    def validate_services(cls, v):
        valid = {
            "VehiclePosition",
            "TripUpdate",
            "Alert",
            "TripModifications",
            "TripStopEvent",
        }
        invalid = sorted(set(v) - valid)
        if invalid:
            raise ValueError(f"Invalid Iceberg services: {', '.join(invalid)}")
        return v


class GtfsRtConfig(_Model):
    """Main configuration for the GTFS-RT fetcher and aggregator."""

    storage: StorageConfig = Field(..., description="Global storage configuration")
    providers: List[ProviderConfig] = Field(..., description="List of providers")
    runtime: RuntimeConfig = Field(
        default_factory=RuntimeConfig, description="How the pipeline runs"
    )
    raw: RawConfig = Field(default_factory=RawConfig, description="Raw archive")
    iceberg: Optional[IcebergConfig] = Field(
        None, description="Iceberg tables over the compacted days"
    )
    output: OutputConfig = Field(
        default_factory=OutputConfig, description="Output configuration"
    )
    base_dir: Optional[str] = Field(
        None,
        description="Folder relative adapter paths start from: the configuration file's (set by the loader; the working directory if None)",
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

    @model_validator(mode="after")
    def validate_trip_stop_events(self):
        if self.output.trip_stop_events:
            import importlib.util

            if importlib.util.find_spec("polars") is None:
                raise ValueError(
                    "trip_stop_events needs Polars: pip install 'gtfs_rt_aggregator[static]'"
                )
            for provider in self.providers:
                statics = {
                    getattr(provider.static_for(api), "name", None)
                    for api in provider.realtime
                    if "TripUpdate" in api.services
                }
                if len(statics) > 1:
                    raise ValueError(
                        f"trip_stop_events: the TripUpdate feeds of provider {provider.name} must use the same static feed"
                    )
        return self

    @model_validator(mode="after")
    def validate_iceberg(self):
        if self.iceberg is not None:
            if not self.output.compact_daily:
                raise ValueError(
                    "[iceberg] registers compacted days: set compact_daily = true in [output]"
                )
            import importlib.util

            if importlib.util.find_spec("pyiceberg") is None:
                raise ValueError(
                    "[iceberg] needs PyIceberg: pip install 'gtfs_rt_aggregator[iceberg]'"
                )
        return self
