from pathlib import Path
from typing import Union, Dict

from .config.loader import load_config_from_toml
from .config.models import GtfsRtConfig
from .storage import create_storage
from .storage.base import StorageInterface, storage_for
from .utils.log_helper import setup_logger
from .runtime.runtime import Runtime
from .utils.redact import install_redaction


class GtfsRtPipeline:
    """Main GTFS-RT pipeline class."""

    def __init__(self, config: GtfsRtConfig):
        """
        Initialize the pipeline.

        @param config: Configuration
        """
        self.logger = setup_logger(f"{__name__}.GtfsRtPipeline")
        # Hide API keys (URL query strings, headers) in the logs of the
        # handlers configured so far
        install_redaction()
        self.config = config
        self.storages = create_storages(config)
        self.runtime = None

    def start(self):
        """Run the pipeline until stopped (Ctrl+C, SIGTERM or stop())."""
        self.runtime = Runtime(self.config, self.storages)
        self.runtime.run()

    def stop(self):
        """Stop the pipeline."""
        if self.runtime is not None:
            self.runtime.stop()


def create_storages(config: GtfsRtConfig) -> Dict[str, StorageInterface]:
    """
    Storage interface of each provider that has its own, and the global one
    under "global".
    """
    storages = {
        "global": create_storage(
            storage_type=config.storage.type, **config.storage.params
        )
    }
    for provider in config.providers:
        if provider.storage:
            storages[provider.name] = create_storage(
                storage_type=provider.storage.type, **provider.storage.params
            )
    return storages


def scrub_static_urls(config: GtfsRtConfig) -> int:
    """
    Remove query strings (which may hold API keys) from the URLs saved in the
    static feeds' manifests by versions before 0.5.1. Returns how many files
    were rewritten.
    """
    from .static.service import scrub_urls, static_base

    rewritten = 0
    storages = create_storages(config)
    for provider in config.providers:
        storage = storage_for(storages, provider.name)
        for feed in provider.static:
            rewritten += scrub_urls(storage, static_base(provider.name, feed.name))
    return rewritten


def run_pipeline(config: GtfsRtConfig):
    """
    Run the GTFS-RT pipeline.

    @param config: Configuration
    """
    logger = setup_logger(f"{__name__}.run_pipeline")
    logger.debug("Creating pipeline instance")
    pipeline = GtfsRtPipeline(config)
    logger.debug("Starting pipeline")
    pipeline.start()


def run_pipeline_from_toml(toml_path: Union[str, Path]):
    """
    Run the GTFS-RT pipeline from a TOML file.

    @param toml_path: Path to the TOML file
    """
    logger = setup_logger(f"{__name__}.run_pipeline_from_toml")
    logger.info(f"Loading configuration from {toml_path}")
    try:
        config = load_config_from_toml(toml_path)
    except Exception as e:
        logger.error(f"Failed to load configuration from {toml_path}: {e}")
        raise
    run_pipeline(config)
