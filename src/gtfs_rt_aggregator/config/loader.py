import tomllib
from pathlib import Path
from typing import Dict, Any, Union, BinaryIO

from ..config.models import (
    GtfsRtConfig,
    StorageConfig,
    ProviderConfig,
    ApiConfig,
    OutputConfig,
)
from ..utils.log_helper import setup_logger

logger = setup_logger(__name__)


def load_config_from_toml(toml_path: Union[str, Path]) -> GtfsRtConfig:
    """
    Load configuration from a TOML file.

    @param toml_path: Path to the TOML file
    @return GtfsRtConfig object
    @raises FileNotFoundError: If the file doesn't exist
    @raises ValueError: If the configuration is invalid
    """
    logger.info(f"Loading configuration from TOML file: {toml_path}")
    try:
        with open(toml_path, "rb") as f:
            config = load_config_from_toml_file(f)
            logger.info(
                f"Successfully loaded configuration with {len(config.providers)} providers"
            )
            return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {toml_path}")
        raise
    except Exception as e:
        logger.error(
            f"Error loading configuration from {toml_path}: {str(e)}", exc_info=True
        )
        raise


def load_config_from_toml_file(toml_file: BinaryIO) -> GtfsRtConfig:
    """
    Load configuration from a TOML file object.

    @param toml_file: File object for the TOML file
    @return GtfsRtConfig object
    @raises ValueError: If the configuration is invalid
    """
    logger.debug("Loading configuration from TOML file object")
    try:
        # Parse TOML
        config_dict = tomllib.load(toml_file)
        logger.debug("Successfully parsed TOML file")

        # Convert to Pydantic model
        return _convert_toml_to_config(config_dict)
    except Exception as e:
        logger.error(
            f"Error loading configuration from file object: {str(e)}", exc_info=True
        )
        raise ValueError(f"Error loading configuration: {e}")


def _convert_toml_to_config(config_dict: Dict[str, Any]) -> GtfsRtConfig:
    """
    Convert a TOML dictionary to a GtfsRtConfig object.

    @param config_dict: Dictionary from parsed TOML
    @return GtfsRtConfig object
    @raises ValueError: If the configuration is invalid
    """
    logger.debug("Converting TOML dictionary to GtfsRtConfig")

    # Extract global storage configuration
    storage_dict = config_dict.get("storage", {})
    storage_type = storage_dict.get("type")
    storage_params = storage_dict.get("params", {})

    if not storage_type:
        logger.error("Missing required field: storage.type")
        raise ValueError("Missing required field: storage.type")

    logger.debug(f"Global storage configuration: type={storage_type}")
    storage_config = StorageConfig(type=storage_type, params=storage_params)

    # Extract output format configuration
    output_dict = config_dict.get("output", {})
    output_filename_format = output_dict.get(
        "filename_format", "{group_time}_to_{next_period}.parquet"
    )
    output_time_format = output_dict.get("time_format", "%H-%M-%S")

    logger.debug(
        f"Output configuration: filename_format={output_filename_format}, time_format={output_time_format}"
    )

    output_config = OutputConfig(
        filename_format=output_filename_format, time_format=output_time_format
    )

    # Extract provider configurations
    providers_list = config_dict.get("providers", [])
    logger.debug(f"Found {len(providers_list)} providers in configuration")
    providers = []

    for provider_dict in providers_list:
        name = provider_dict.get("name")
        if not name:
            logger.error("Missing required field: provider.name")
            raise ValueError("Missing required field: provider.name")

        logger.debug(f"Processing provider: {name}")

        # Extract provider-specific storage if defined
        provider_storage = None
        if "storage" in provider_dict:
            provider_storage_dict = provider_dict.get("storage", {})
            provider_storage_type = provider_storage_dict.get("type")
            provider_storage_params = provider_storage_dict.get("params", {})

            if not provider_storage_type:
                logger.error(
                    f"Missing required field: provider.storage.type for provider {name}"
                )
                raise ValueError(
                    f"Missing required field: provider.storage.type for provider {name}"
                )

            logger.debug(
                f"Provider-specific storage for {name}: type={provider_storage_type}"
            )
            provider_storage = StorageConfig(
                type=provider_storage_type, params=provider_storage_params
            )

        # Extract API configurations
        apis_list = provider_dict.get("apis", [])
        logger.debug(f"Found {len(apis_list)} APIs for provider {name}")
        apis = []

        for api_dict in apis_list:
            url = api_dict.get("url")
            if not url:
                logger.error(
                    f"Missing required field: provider.apis.url for provider {name}"
                )
                raise ValueError(
                    f"Missing required field: provider.apis.url for provider {name}"
                )

            services = api_dict.get("services", [])
            if not services:
                logger.error(
                    f"Missing required field: provider.apis.services for provider {name} and URL {url}"
                )
                raise ValueError(
                    f"Missing required field: provider.apis.services for provider {name} and URL {url}"
                )

            refresh_seconds = api_dict.get("refresh_seconds", 60)
            frequency_minutes = api_dict.get("frequency_minutes", 60)
            check_interval_seconds = api_dict.get("check_interval_seconds", 300)
            accumulate_count = api_dict.get("accumulate_count", 0)

            logger.debug(
                f"API for {name}: url={url}, services={services}, refresh={refresh_seconds}s, frequency={frequency_minutes}m, check_interval={check_interval_seconds}s"
            )

            api = ApiConfig(
                url=url,
                services=services,
                refresh_seconds=refresh_seconds,
                frequency_minutes=frequency_minutes,
                check_interval_seconds=check_interval_seconds,
                accumulate_count=accumulate_count,
            )

            apis.append(api)

        if not apis:
            logger.error(f"No APIs defined for provider {name}")
            raise ValueError(f"No APIs defined for provider {name}")

        timezone = provider_dict.get("timezone", "UTC")
        logger.debug(f"Provider {name} timezone: {timezone}")

        provider = ProviderConfig(
            name=name,
            timezone=timezone,
            apis=apis,
            frequency_minutes=provider_dict.get("frequency_minutes"),
            check_interval_seconds=provider_dict.get("check_interval_seconds"),
            storage=provider_storage,
        )

        providers.append(provider)

    if not providers:
        logger.error("No providers defined in configuration")
        raise ValueError("No providers defined")

    # Create the config
    logger.info(f"Successfully created configuration with {len(providers)} providers")
    return GtfsRtConfig(
        storage=storage_config, providers=providers, output=output_config
    )
