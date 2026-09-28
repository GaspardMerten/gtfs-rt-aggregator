import os
import re
import tomllib
from pathlib import Path
from typing import Dict, Any, Union, BinaryIO

from ..config.models import (
    GtfsRtConfig,
    StorageConfig,
    ProviderConfig,
    ApiConfig,
    OutputConfig,
    StaticConfig,
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
        return _convert_toml_to_config(expand_env(config_dict))
    except Exception as e:
        logger.error(
            f"Error loading configuration from file object: {str(e)}", exc_info=True
        )
        raise ValueError(f"Error loading configuration: {e}")


_ENV_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env(value: Any, where: str = "") -> Any:
    """
    Replace ${NAME} with the NAME environment variable in every string value,
    so secrets such as API keys can stay out of the file. "$${" writes a
    literal "${".

    @raises ValueError: If a referenced variable is not set
    """
    if isinstance(value, dict):
        return {
            k: expand_env(v, f"{where}.{k}" if where else k) for k, v in value.items()
        }
    if isinstance(value, list):
        return [expand_env(v, f"{where}[{i}]") for i, v in enumerate(value)]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise ValueError(
                f"Environment variable {name} (used in {where}) is not set"
            )
        return os.environ[name]

    parts = value.split("$${")
    return "${".join(_ENV_REFERENCE.sub(replace, part) for part in parts)


def _output_config(output_dict: Dict[str, Any]) -> OutputConfig:
    """Output options, converting filename_format / time_format (before 0.5.0)."""
    output_dict = dict(output_dict)
    legacy = {
        k: output_dict.pop(k)
        for k in ("filename_format", "time_format")
        if k in output_dict
    }
    if legacy:
        if "path_template" in output_dict:
            raise ValueError(
                "Use path_template or filename_format/time_format, not both"
            )
        logger.warning(
            "[output] filename_format and time_format are deprecated, use path_template"
        )
        time_format = legacy.get("time_format", "%H-%M-%S")
        filename = legacy.get(
            "filename_format", "{group_time}_to_{next_period}.parquet"
        )
        output_dict["path_template"] = (
            "{provider}/{service}/{start:%Y-%m-%d}/"
            + filename.replace("{group_time}", "{start:" + time_format + "}").replace(
                "{next_period}", "{end:" + time_format + "}"
            )
        )
    return OutputConfig(**output_dict)


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

    output_config = _output_config(config_dict.get("output", {}))
    logger.debug(f"Output configuration: {output_config}")

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

        # Extract realtime feed configurations ("apis" before 0.3.0)
        if "realtime" in provider_dict and "apis" in provider_dict:
            raise ValueError(
                f"Provider {name} defines both realtime and apis: use realtime only"
            )
        if "apis" in provider_dict:
            logger.warning(
                f"Provider {name}: [[providers.apis]] is deprecated, rename it to [[providers.realtime]]"
            )
        apis_list = provider_dict.get("realtime", provider_dict.get("apis", []))
        logger.debug(f"Found {len(apis_list)} realtime feeds for provider {name}")
        apis = []

        for api_dict in apis_list:
            url = api_dict.get("url")
            if not url:
                logger.error(
                    f"Missing required field: provider.realtime.url for provider {name}"
                )
                raise ValueError(
                    f"Missing required field: provider.realtime.url for provider {name}"
                )

            services = api_dict.get("services", [])
            if not services:
                logger.error(
                    f"Missing required field: provider.realtime.services for provider {name} and URL {url}"
                )
                raise ValueError(
                    f"Missing required field: provider.realtime.services for provider {name} and URL {url}"
                )

            api = ApiConfig(**api_dict)
            logger.debug(f"Realtime feed for {name}: {api.url} {api.services}")

            apis.append(api)

        # Extract static feed configurations
        static_feeds = []
        for static_dict in provider_dict.get("static", []):
            logger.debug(
                f"Static feed for {name}: {static_dict.get('url') or static_dict.get('index_url')}"
            )
            static_feeds.append(StaticConfig(**static_dict))

        timezone = provider_dict.get("timezone", "UTC")
        logger.debug(f"Provider {name} timezone: {timezone}")

        provider = ProviderConfig(
            name=name,
            timezone=timezone,
            realtime=apis,
            static=static_feeds,
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
