"""Integration test for the full GTFS-RT pipeline."""

import time
import unittest
import logging
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from unittest.mock import patch, MagicMock

import pandas as pd
import pytz

from src.gtfs_rt_aggregator.aggregator.service import AggregatorService
from src.gtfs_rt_aggregator.config.models import (
    GtfsRtConfig,
    ProviderConfig,
    ApiConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.fetcher.service import FetcherService
from src.gtfs_rt_aggregator.pipeline import GtfsRtPipeline
from tests.mocks import MockStorageInterface, MockServerManager


class TestFullPipeline(unittest.TestCase):
    """Test the full GTFS-RT pipeline from fetching to aggregation."""

    @classmethod
    def setUpClass(cls):
        """Start the mock server before tests."""
        cls.server_manager = MockServerManager()
        success = cls.server_manager.start()
        if not success:
            raise RuntimeError("Could not start mock server")
        # Get the actual port that was used
        cls.mock_server_port = cls.server_manager.port

    @classmethod
    def tearDownClass(cls):
        """Stop the mock server after tests."""
        cls.server_manager.stop()

    def setUp(self):
        """Set up test configuration and storage."""
        # Create mock storage
        self.storage = MockStorageInterface()
        self.provider_name = "test_provider"

        # Create storages map with both global and provider-specific storage
        self.storages = {"global": self.storage, self.provider_name: self.storage}

        # Create test configuration with short intervals for quick testing
        self.config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={}),
            providers=[
                ProviderConfig(
                    name=self.provider_name,
                    timezone="UTC",
                    apis=[
                        ApiConfig(
                            url=f"http://localhost:{self.__class__.mock_server_port}/vehicle_positions",
                            refresh_seconds=5,  # Short for testing
                            services=["VehiclePosition"],
                            frequency_minutes=5,  # Short for testing
                        ),
                        ApiConfig(
                            url=f"http://localhost:{self.__class__.mock_server_port}/trip_updates",
                            refresh_seconds=5,  # Short for testing
                            services=["TripUpdate"],
                            frequency_minutes=5,  # Short for testing
                        ),
                        ApiConfig(
                            url=f"http://localhost:{self.__class__.mock_server_port}/alerts",
                            refresh_seconds=5,  # Short for testing
                            services=["Alert"],
                            frequency_minutes=5,  # Short for testing
                        ),
                    ],
                )
            ],
        )

    def test_fetch_and_aggregate(self):
        """Test the full pipeline: fetching GTFS-RT data and then aggregating it."""
        # Set up a log handler that writes to a StringIO buffer

        # Get the root logger and add the handler
        root_logger = logging.getLogger()

        # 1. Create and run the fetcher service
        fetcher = FetcherService(self.config, self.storages)

        # Run the fetcher for each service type
        for api in self.config.providers[0].apis:
            fetcher.run_once(
                provider_name=self.provider_name,
                url=api.url,
                service_types=api.services,
                timezone="UTC",
            )

        # Verify that individual files were created
        for service_type in ["VehiclePosition", "TripUpdate", "Alert"]:
            files = self.storage.list_files(
                f"{self.provider_name}/{service_type}/individual"
            )
            self.assertTrue(
                len(files) > 0, f"No individual files created for {service_type}"
            )

            # Verify that the files can be read as DataFrames
            for file_path in files:
                data_bytes = self.storage.read_bytes(file_path)
                df = pd.read_parquet(BytesIO(data_bytes))
                self.assertFalse(df.empty)
                self.assertIn("fetch_time", df.columns)
                # The actual data doesn't have a provider column, so we don't test for it

        # We'll create a few more timestamps for testing aggregation
        now = datetime.now(pytz.UTC)
        now_date = now.strftime("%Y-%m-%d")

        # Create additional fetches with different timestamps
        for i in range(1, 3):  # Create 2 more files for each type
            for api in self.config.providers[0].apis:
                for service_type in api.services:
                    # Simulate a fetch 5 minutes apart
                    timestamp = now - timedelta(minutes=i * 5)
                    timestamp_str = timestamp.strftime("%Y-%m-%d_%H-%M-%S")
                    file_path = f"{self.provider_name}/{service_type}/individual/individual_{timestamp_str}.parquet"

                    # Use the first file we created earlier as a template
                    template_path = self.storage.list_files(
                        f"{self.provider_name}/{service_type}/individual"
                    )[0]
                    template_data = self.storage.read_bytes(template_path)

                    # Read the template as DataFrame, update timestamps, and save
                    df = pd.read_parquet(BytesIO(template_data))
                    df["fetch_time"] = timestamp

                    # Convert back to bytes and save
                    buffer = BytesIO()
                    df.to_parquet(buffer)
                    buffer.seek(0)
                    self.storage.save_bytes(buffer.read(), file_path)

        # 3. Run the aggregator service
        aggregator = AggregatorService(self.config, self.storages)

        # Run aggregation for each service type
        for service_type in ["VehiclePosition", "TripUpdate", "Alert"]:
            aggregator.run_once(
                provider_name=self.provider_name,
                service_types=[service_type],
                frequency_minutes=5,
                timezone="UTC",
            )

        # 4. Verify that aggregated files were created
        for service_type in ["VehiclePosition", "TripUpdate", "Alert"]:
            # Check for hourly aggregated files
            aggregated_files = self.storage.list_files(
                f"{self.provider_name}/{service_type}/{now_date}/"
            )
            self.assertTrue(
                len(aggregated_files) > 0,
                f"No hourly aggregated files created for {service_type}",
            )

            # Verify that the aggregated files can be read as DataFrames
            for file_path in aggregated_files:
                data_bytes = self.storage.read_bytes(file_path)
                df = pd.read_parquet(BytesIO(data_bytes))
                self.assertFalse(df.empty)
                self.assertIn("fetch_time", df.columns)
                # The actual data might not have a provider column, so we don't test for it

    def test_pipeline_integration(self):
        """
        Test the GtfsRtPipeline class that orchestrates the entire flow.
        """
        # Mock the storage factory and scheduler
        with patch("src.gtfs_rt_aggregator.storage.create_storage") as mock_create, patch(
            "src.gtfs_rt_aggregator.utils.scheduler.SchedulerClass"
        ) as mock_scheduler_class:

            # Configure storage mock
            mock_create.return_value = self.storage

            # Configure scheduler mock
            mock_scheduler = MagicMock()
            mock_scheduler_class.return_value = mock_scheduler

            # Create the pipeline
            pipeline = GtfsRtPipeline(self.config, mock_scheduler)

            # Verify that services were created correctly
            self.assertIsNotNone(pipeline.fetcher_service)
            self.assertIsNotNone(pipeline.aggregator_service)
            self.assertIsNotNone(pipeline.scheduler)

            # Start the pipeline (this should set up scheduling but not actually run because of our mock)
            pipeline.start()

            # Verify that add_schedules was called at least once
            # The exact number of calls can vary based on implementation
            self.assertTrue(mock_scheduler.add_schedules.call_count > 0)

            # Verify scheduler was started
            mock_scheduler.start.assert_called_once()

            # Stop the pipeline
            pipeline.stop()

            # Verify scheduler was stopped
            mock_scheduler.stop.assert_called_once()


if __name__ == "__main__":
    unittest.main()
