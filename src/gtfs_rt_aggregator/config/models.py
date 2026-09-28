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

    @classmethod
    @field_validator("type")
    def validate_storage_type(cls, v):
        """
        Validate storage type.

        @param v: Storage type value to validate
        @return Validated storage type
        @raises ValueError: If storage type is invalid
        """
        valid_types = {"filesystem", "gcs", "google", "google_cloud_storage"}
        if v.lower() not in valid_types:
            raise ValueError(
                f"Invalid storage type: {v}. Must be one of: {', '.join(valid_types)}"
            )
        return v.lower()

    @classmethod
    @field_validator("params")
    def validate_params(cls, values):
        """
        Validate storage parameters based on type.

        @param values: Dictionary of values to validate
        @return Validated values
        @raises ValueError: If required parameters are missing
        """
        storage_type = values.get("type")
        params = values.get("params", {})

        if storage_type in ("gcs", "google", "google_cloud_storage"):
            if "bucket_name" not in params:
                raise ValueError("bucket_name is required for Google Cloud Storage")

        return values


class ApiConfig(BaseModel):
    """GTFS-RT (realtime) feed configuration for a provider."""

    url: str = Field(..., description="URL of the GTFS-RT feed")
    services: List[str] = Field(
        ...,
        description="List of service types to fetch (VehiclePosition, TripUpdate, Alert, TripModifications)",
    )
    refresh_seconds: int = Field(
        60, description="How often to fetch data from this API (in seconds)"
    )
    frequency_minutes: int = Field(
        60, description="How often to group data (in minutes)"
    )
    check_interval_seconds: int = Field(
        300, description="How often to check for new files to aggregate (in seconds)"
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

    @classmethod
    @field_validator("services")
    def validate_services(cls, v):
        """
        Validate service types.

        @param v: List of service types to validate
        @return Validated service types
        @raises ValueError: If service type is invalid
        """
        valid_services = {"VehiclePosition", "TripUpdate", "Alert", "TripModifications"}
        for service in v:
            if service not in valid_services:
                raise ValueError(
                    f"Invalid service type: {service}. Must be one of: {', '.join(valid_services)}"
                )
        return v

    @classmethod
    @field_validator("refresh_seconds", "frequency_minutes", "check_interval_seconds")
    def validate_time_values(cls, v, values, field):
        """
        Validate time values are positive.

        @param v: Time value to validate
        @param values: Dictionary of values
        @param field: Field being validated
        @return Validated time value
        @raises ValueError: If time value is not positive
        """
        if v <= 0:
            raise ValueError(f"{field.name} must be positive")
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
        None, description="Default grouping frequency for all APIs (in minutes)"
    )
    check_interval_seconds: Optional[int] = Field(
        None, description="Default check interval for all APIs (in seconds)"
    )
    storage: Optional[StorageConfig] = Field(
        None, description="Provider-specific storage configuration (overrides global)"
    )

    @classmethod
    @field_validator("frequency_minutes", "check_interval_seconds")
    def validate_time_values(cls, v, values, field):
        """
        Validate time values are positive.

        @param v: Time value to validate
        @param values: Dictionary of values
        @param field: Field being validated
        @return Validated time value
        @raises ValueError: If time value is not positive
        """
        if v is not None and v <= 0:
            raise ValueError(f"{field.name} must be positive")
        return v

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

    @classmethod
    @field_validator("timezone")
    def validate_timezone(cls, v):
        """
        Validate timezone.

        @param v: Timezone to validate
        @return Validated timezone
        @raises ValueError: If timezone is invalid
        """
        try:
            import pytz

            pytz.timezone(v)
        except Exception as e:
            raise ValueError(f"Invalid timezone: {v}. {str(e)}")


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

    @classmethod
    @field_validator("providers")
    def validate_provider_names(cls, providers):
        """
        Validate provider names are unique.

        @param providers: List of providers to validate
        @return Validated providers
        @raises ValueError: If provider names are not unique
        """
        names = [p.name for p in providers]
        if len(names) != len(set(names)):
            raise ValueError("Provider names must be unique")
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
