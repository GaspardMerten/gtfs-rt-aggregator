from pathlib import Path
from typing import Union, Dict, Optional

from .aggregator.service import AggregatorService
from .config.loader import load_config_from_toml
from .config.models import GtfsRtConfig
from .fetcher.service import FetcherService
from .static.service import StaticService
from .storage import create_storage
from .storage.base import StorageInterface
from .utils.log_helper import setup_logger
from .runtime.runtime import Runtime
from .utils.cleanup import clean_stale_temp_files
from .utils.redact import install_redaction
from .utils.scheduler import SchedulerClass


class GtfsRtPipeline:
    """Main GTFS-RT pipeline class."""

    def __init__(
        self, config: GtfsRtConfig, scheduler: Optional[SchedulerClass] = None
    ):
        """
        Initialize the pipeline.

        @param config: Configuration
        """
        self.logger = setup_logger(f"{__name__}.GtfsRtPipeline")
        # Hide API keys (URL query strings, headers) in the logs of the
        # handlers configured so far
        install_redaction()
        self.logger.debug("Initializing GTFS-RT Pipeline")

        self.config = config

        # Create storage interfaces for each provider and global
        self.storages = self._create_storages()

        # Create services
        self.fetcher_service = FetcherService(config, self.storages)
        self.aggregator_service = AggregatorService(config, self.storages)
        self.static_service = StaticService(config, self.storages)

        # A SchedulerClass selects the pre-0.6.0 way of running (deprecated)
        self.scheduler = scheduler
        self.runtime = None

    def _create_storages(self) -> Dict[str, StorageInterface]:
        return create_storages(self.config)

    def start(self):
        """Run the pipeline until stopped (Ctrl+C, SIGTERM or stop())."""
        if self.scheduler is None:
            self.runtime = Runtime(self.config, self.storages)
            self.runtime.run()
            return
        self._start_legacy()

    def stop(self):
        """Stop the pipeline."""
        if self.scheduler is None:
            if self.runtime is not None:
                self.runtime.stop()
            return
        try:
            self.scheduler.stop()
        except Exception as e:
            self.logger.error(f"Error stopping pipeline: {str(e)}", exc_info=True)

    def _start_legacy(self):
        """Before 0.6.0: one process per job, started by a SchedulerClass."""
        self.logger.warning(
            "Running with a SchedulerClass (one process per job) is deprecated; "
            "without one, the pipeline uses the disk-spool runtime"
        )
        removed = clean_stale_temp_files()
        if removed:
            self.logger.info(f"Removed {removed} stale temporary files or folders")
        try:
            self.scheduler.add_schedules(self.fetcher_service.get_scheduling())
            self.scheduler.add_schedules(self.aggregator_service.get_scheduling())
            self.scheduler.add_schedules(self.static_service.get_scheduling())
            self.scheduler.start()
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            self.logger.error(f"Error starting pipeline: {str(e)}", exc_info=True)
            self.stop()


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
    for provider in config.providers:
        storage_config = provider.storage or config.storage
        storage = create_storage(
            storage_type=storage_config.type, **storage_config.params
        )
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
        run_pipeline(config)
    except Exception as e:
        logger.error(
            f"Failed to load configuration from {toml_path}: {str(e)}", exc_info=True
        )
        raise
