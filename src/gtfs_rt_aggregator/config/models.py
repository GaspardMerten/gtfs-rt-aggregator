from typing import List, Optional, Dict, Any

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


class ApiConfig(BaseModel):
    """GTFS-RT (realtime) feed configuration for a provider."""

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

    url: str = Field(..., description="URL of the GTFS zip")
    name: str = Field(
        "static",
        min_length=1,
        description="Folder the versions are stored in, under the provider folder",
    )
    check_minutes: int = Field(
        60, gt=0, description="How often to check for a new version (in minutes)"
    )
    headers: Dict[str, str] = Field(
        default_factory=dict,
        description="HTTP headers sent with each request (e.g. an API key)",
    )


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
        return self

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


class OutputConfig(BaseModel):
    filename_format: str = Field(
        "{group_time}_to_{next_period}.parquet",
        description="Filename format for output files",
    )

    time_format: str = Field(
        "%H-%M-%S",
        description="Time format for filename timestamps",
    )


class GtfsRtConfig(BaseModel):
    """Main configuration for the GTFS-RT fetcher and aggregator."""

    storage: StorageConfig = Field(..., description="Global storage configuration")
    providers: List[ProviderConfig] = Field(..., description="List of providers")
    output: OutputConfig = Field(
        OutputConfig(
            filename_format="{group_time}_to_{next_period}.parquet",
            time_format="%H-%M-%S",
        ),
        description="Output configuration",
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
