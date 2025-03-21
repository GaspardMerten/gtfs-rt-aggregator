from pathlib import Path
from typing import Union, Dict, Optional

from .aggregator.service import AggregatorService
from .config.loader import load_config_from_toml
from .config.models import GtfsRtConfig
from .fetcher.service import FetcherService
from .storage import create_storage
from .storage.base import StorageInterface
from .utils.log_helper import setup_logger
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
        self.logger.debug("Initializing GTFS-RT Pipeline")

        self.config = config

        # Create storage interfaces for each provider and global
        self.storages = self._create_storages()

        # Create services
        self.fetcher_service = FetcherService(config, self.storages)
        self.aggregator_service = AggregatorService(config, self.storages)

        # Create scheduler
        self.scheduler = scheduler or SchedulerClass()
        self.logger.debug("Pipeline initialization complete")

    def _create_storages(self) -> Dict[str, StorageInterface]:
        """
        Create storage interfaces for each provider and global.

        @return Dictionary with provider names as keys and storage interfaces as values,
                with a special key 'global' for the global storage.
        """
        self.logger.debug("Creating storage interfaces")
        storages = {}

        # Create global storage
        self.logger.debug("Creating global storage")
        global_storage = create_storage(
            storage_type=self.config.storage.type, **self.config.storage.params
        )
        storages["global"] = global_storage

        # Create provider-specific storages if defined
        for provider in self.config.providers:
            if provider.storage:
                self.logger.debug(f"Creating storage for provider: {provider.name}")
                provider_storage = create_storage(
                    storage_type=provider.storage.type, **provider.storage.params
                )
                storages[provider.name] = provider_storage

        self.logger.debug(f"Created {len(storages)} storage interfaces")
        return storages

    def start(self):
        """Start the pipeline."""
        self.logger.info("Starting GTFS-RT Pipeline")

        try:
            # Get schedules from services
            self.logger.debug("Getting fetcher schedules")
            fetcher_schedules = self.fetcher_service.get_scheduling()

            self.logger.debug("Getting aggregator schedules")
            aggregator_schedules = self.aggregator_service.get_scheduling()

            # Add schedules to the scheduler
            self.logger.debug("Adding schedules to scheduler")
            self.scheduler.add_schedules(fetcher_schedules)
            self.scheduler.add_schedules(aggregator_schedules)

            self.logger.info("Pipeline started. Press Ctrl+C to stop.")
            self.scheduler.start()
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
            self.stop()
        except Exception as e:
            self.logger.error(f"Error starting pipeline: {str(e)}", exc_info=True)
            self.stop()

    def stop(self):
        """Stop the pipeline."""
        self.logger.info("Stopping pipeline...")

        try:
            # Stop the scheduler
            self.scheduler.stop()
            self.logger.info("Pipeline stopped successfully")
        except Exception as e:
            self.logger.error(f"Error stopping pipeline: {str(e)}", exc_info=True)


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
