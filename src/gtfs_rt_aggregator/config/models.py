from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field, field_validator


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
    """API configuration for a provider."""

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
    accumulate_count: Optional[int] = 0

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


class ProviderConfig(BaseModel):
    """Provider configuration."""

    name: str = Field(..., description="Name of the provider")
    timezone: str = Field("UTC", description="Timezone of the provider")
    apis: List[ApiConfig] = Field(..., description="List of APIs for this provider")
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
        for a in provider.apis:
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
