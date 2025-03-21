import os
import unittest
from io import BytesIO

import pandas as pd

from gtfs_rt_aggregator.config.models import (
    GtfsRtConfig,
    ProviderConfig,
    ApiConfig,
    StorageConfig,
)
from gtfs_rt_aggregator.fetcher.service import FetcherService
from tests.mocks import MockStorageInterface, MockServerManager

# Start with a base port, but the actual port may change
BASE_PORT = int(os.environ.get("MOCKUP_SERVER_PORT", 8788))


class TestFetcherService(unittest.TestCase):
    """Tests for the FetcherService."""

    @classmethod
    def setUpClass(cls):
        """Start the mock server before tests."""
        cls.server_manager = MockServerManager(BASE_PORT)
        success = cls.server_manager.start()
        if not success:
            raise RuntimeError("Could not start mock server")
        # Get the actual port that was used (may be different from BASE_PORT)
        cls.mock_server_port = cls.server_manager.port

    @classmethod
    def tearDownClass(cls):
        """Stop the mock server after tests."""
        cls.server_manager.stop()

    def setUp(self):
        """Set up test configuration and storage."""
        # Create mock storage
        self.storage = MockStorageInterface()
        self.storages = {"global": self.storage}

        # Create test configuration
        self.config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={}),
            providers=[
                ProviderConfig(
                    name="test_provider",
                    timezone="UTC",
                    apis=[
                        ApiConfig(
                            url=f"http://localhost:{self.__class__.mock_server_port}/alerts",
                            refresh_seconds=60,
                            services=["Alert"],
                        ),
                        ApiConfig(
                            url=f"http://localhost:{self.__class__.mock_server_port}/trip_updates",
                            refresh_seconds=60,
                            services=["TripUpdate"],
                        ),
                        ApiConfig(
                            url=f"http://localhost:{self.__class__.mock_server_port}/vehicle_positions",
                            refresh_seconds=60,
                            services=["VehiclePosition"],
                        ),
                    ],
                )
            ],
        )

        # Create the fetcher service
        self.fetcher_service = FetcherService(self.config, self.storages)

    def test_get_scheduling(self):
        """Test that scheduling is correctly generated."""
        schedules = self.fetcher_service.get_scheduling()

        # Should have 3 schedules (one for each API)
        self.assertEqual(len(schedules), 3)

        # Each schedule should be a tuple with 4 elements
        for schedule in schedules:
            self.assertEqual(len(schedule), 4)

            # First element should be the refresh seconds (60)
            self.assertEqual(schedule[0], 60)

            # Second element should be the run_once method
            self.assertEqual(schedule[1], self.fetcher_service.run_once)

            # Third element should be a name string
            self.assertIsInstance(schedule[2], str)

            # Fourth element should be a dict with arguments
            self.assertIsInstance(schedule[3], dict)
            self.assertIn("provider_name", schedule[3])
            self.assertIn("url", schedule[3])
            self.assertIn("service_types", schedule[3])
            self.assertIn("timezone", schedule[3])

    def test_run_once_alerts(self):
        """Test fetching alerts."""
        # Clear storage before test
        self.storage.saved_data = {}

        # Run the fetch job for alerts
        self.fetcher_service.run_once(
            provider_name="test_provider",
            url=f"http://localhost:{self.__class__.mock_server_port}/alerts",
            service_types=["Alert"],
            timezone="UTC",
        )

        # Check that data was saved to storage
        saved_paths = self.storage.list_paths()

        # Should have one saved file
        self.assertEqual(len(saved_paths), 1)

        # Path should match the expected format
        path = saved_paths[0]
        self.assertTrue(
            path.startswith("test_provider/Alert/individual/"),
            f"Path format incorrect: {path}",
        )
        self.assertTrue(path.endswith(".parquet"), f"File extension incorrect: {path}")

        # Verify the saved data can be loaded as a DataFrame
        data_bytes = self.storage.get_bytes(path)
        df = pd.read_parquet(BytesIO(data_bytes))

        # Should not be empty
        self.assertFalse(df.empty)

        # Should have the expected columns for alerts
        self.assertIn("fetch_time", df.columns)

    def test_run_once_trip_updates(self):
        """Test fetching trip updates."""
        # Clear storage before test
        self.storage.saved_data = {}

        # Run the fetch job for trip updates
        self.fetcher_service.run_once(
            provider_name="test_provider",
            url=f"http://localhost:{self.__class__.mock_server_port}/trip_updates",
            service_types=["TripUpdate"],
            timezone="UTC",
        )

        # Check that data was saved to storage
        saved_paths = self.storage.list_paths()

        # Should have one saved file
        self.assertEqual(len(saved_paths), 1)

        # Path should match the expected format
        path = saved_paths[0]
        self.assertTrue(
            path.startswith("test_provider/TripUpdate/individual/"),
            f"Path format incorrect: {path}",
        )
        self.assertTrue(path.endswith(".parquet"), f"File extension incorrect: {path}")

        # Verify the saved data can be loaded as a DataFrame
        data_bytes = self.storage.get_bytes(path)
        df = pd.read_parquet(BytesIO(data_bytes))

        # Should not be empty
        self.assertFalse(df.empty)

        # Should have the expected columns for trip updates
        self.assertIn("fetch_time", df.columns)

    def test_run_once_vehicle_positions(self):
        """Test fetching vehicle positions."""
        # Clear storage before test
        self.storage.saved_data = {}

        # Run the fetch job for vehicle positions
        self.fetcher_service.run_once(
            provider_name="test_provider",
            url=f"http://localhost:{self.__class__.mock_server_port}/vehicle_positions",
            service_types=["VehiclePosition"],
            timezone="UTC",
        )

        # Check that data was saved to storage
        saved_paths = self.storage.list_paths()

        # Should have one saved file
        self.assertEqual(len(saved_paths), 1)

        # Path should match the expected format
        path = saved_paths[0]
        self.assertTrue(
            path.startswith("test_provider/VehiclePosition/individual/"),
            f"Path format incorrect: {path}",
        )
        self.assertTrue(path.endswith(".parquet"), f"File extension incorrect: {path}")

        # Verify the saved data can be loaded as a DataFrame
        data_bytes = self.storage.get_bytes(path)
        df = pd.read_parquet(BytesIO(data_bytes))

        # Should not be empty
        self.assertFalse(df.empty)

        # Should have the expected columns for vehicle positions
        self.assertIn("fetch_time", df.columns)


if __name__ == "__main__":
    unittest.main()
