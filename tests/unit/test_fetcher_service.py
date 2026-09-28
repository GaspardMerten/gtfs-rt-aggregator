import multiprocessing
import os
import pickle
import tempfile
import unittest
from datetime import datetime
from io import BytesIO
from unittest.mock import patch

import pandas as pd
import pytz

from src.gtfs_rt_aggregator.config.models import (
    GtfsRtConfig,
    ProviderConfig,
    ApiConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.fetcher.service import FetcherService
from src.gtfs_rt_aggregator.storage.filesystem import FileSystemStorage
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
        self.assertIn("fetchTime", df.columns)

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
        self.assertIn("fetchTime", df.columns)

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
        self.assertIn("fetchTime", df.columns)


class _FakeDatetime(datetime):
    """Stand-in for datetime in the fetcher service, with a settable now()."""

    current = datetime(2025, 1, 1, 16, 0, 0, tzinfo=pytz.UTC)

    @classmethod
    def now(cls, tz=None):
        return cls.current.astimezone(tz)


class TestFetcherServiceAccumulate(unittest.TestCase):
    """Tests for buffering fetches in memory per clock-aligned window."""

    @classmethod
    def setUpClass(cls):
        cls.server_manager = MockServerManager()
        if not cls.server_manager.start():
            raise RuntimeError("Could not start mock server")
        cls.url = f"http://localhost:{cls.server_manager.port}/vehicle_positions"

    @classmethod
    def tearDownClass(cls):
        cls.server_manager.stop()

    def setUp(self):
        patcher = patch(
            "src.gtfs_rt_aggregator.fetcher.service.datetime", _FakeDatetime
        )
        patcher.start()
        self.addCleanup(patcher.stop)

    def _make_service(self, storage, accumulate_minutes, concatenate=True):
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={}),
            providers=[
                ProviderConfig(
                    name="test_provider",
                    timezone="UTC",
                    apis=[
                        ApiConfig(
                            url=self.url,
                            services=["VehiclePosition"],
                            accumulate_minutes=accumulate_minutes,
                            accumulate_concatenate=concatenate,
                        )
                    ],
                )
            ],
        )
        service = FetcherService(config, {"global": storage})
        self.addCleanup(service.flush_all)
        return service

    def _run(self, service, hour=None, minute=None, second=0):
        if hour is not None:
            _FakeDatetime.current = datetime(
                2025, 1, 1, hour, minute, second, tzinfo=pytz.UTC
            )
        service.run_once(
            provider_name="test_provider",
            url=self.url,
            service_types=["VehiclePosition"],
            timezone="UTC",
        )

    def _rows(self, storage, path):
        return len(pd.read_parquet(BytesIO(storage.get_bytes(path))))

    def _single_fetch_rows(self):
        storage = MockStorageInterface()
        self._run(self._make_service(storage, 0))
        (path,) = storage.list_paths()
        return self._rows(storage, path)

    def test_window_written_by_first_fetch_of_next_window(self):
        storage = MockStorageInterface()
        service = self._make_service(storage, 15)

        self._run(service, 16, 1)
        self._run(service, 16, 14, 59)
        self.assertEqual(storage.list_paths(), [])

        self._run(service, 16, 15)
        prefix = "test_provider/VehiclePosition/individual/"
        self.assertEqual(
            storage.list_paths(), [prefix + "2025-01-01_16-01-00Z.parquet"]
        )
        self.assertEqual(
            self._rows(storage, storage.list_paths()[0]), 2 * self._single_fetch_rows()
        )

    def test_window_written_separately(self):
        storage = MockStorageInterface()
        service = self._make_service(storage, 15, concatenate=False)

        self._run(service, 16, 1)
        self._run(service, 16, 2)
        self.assertEqual(storage.list_paths(), [])
        self._run(service, 16, 15)
        self.assertEqual(len(storage.list_paths()), 2)

    def test_late_fetch_from_previous_window_written_alone(self):
        storage = MockStorageInterface()
        service = self._make_service(storage, 15)

        self._run(service, 16, 15)
        self._run(service, 16, 14, 58)
        prefix = "test_provider/VehiclePosition/individual/"
        self.assertEqual(
            storage.list_paths(), [prefix + "2025-01-01_16-14-58Z.parquet"]
        )

        # The 16:15 window is still buffered
        service.flush_all()
        self.assertIn(prefix + "2025-01-01_16-15-00Z.parquet", storage.list_paths())

    def test_day_window_aligned_on_midnight(self):
        storage = MockStorageInterface()
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={}),
            providers=[
                ProviderConfig(
                    name="test_provider",
                    timezone="UTC",
                    apis=[
                        ApiConfig(
                            url=self.url,
                            services=["VehiclePosition"],
                            frequency_minutes=1440,
                            accumulate_minutes=1440,
                        )
                    ],
                )
            ],
        )
        service = FetcherService(config, {"global": storage})
        self.addCleanup(service.flush_all)

        self._run(service, 0, 0)
        self._run(service, 23, 59, 59)
        self.assertEqual(storage.list_paths(), [])
        _FakeDatetime.current = datetime(2025, 1, 2, 0, 0, 0, tzinfo=pytz.UTC)
        self._run(service)
        self.assertEqual(len(storage.list_paths()), 1)

    def test_flush_all_writes_remaining(self):
        storage = MockStorageInterface()
        service = self._make_service(storage, 15)

        self._run(service, 16, 1)
        self.assertEqual(storage.list_paths(), [])
        service.flush_all()
        self.assertEqual(len(storage.list_paths()), 1)

    def test_invalid_windows_rejected(self):
        for minutes, frequency in ((7, 60), (45, 60), (120, 60)):
            with self.assertRaises(ValueError):
                ApiConfig(
                    url=self.url,
                    services=["VehiclePosition"],
                    frequency_minutes=frequency,
                    accumulate_minutes=minutes,
                )
        ApiConfig(
            url=self.url,
            services=["VehiclePosition"],
            frequency_minutes=60,
            accumulate_minutes=15,
        )

    def test_service_is_picklable(self):
        # spawn/forkserver start methods pickle the job target (self.run_once)
        service = self._make_service(MockStorageInterface(), 15)
        pickle.dumps(service.run_once)

    def test_concurrent_jobs_lose_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = FileSystemStorage(directory)
            service = self._make_service(storage, 15)

            _FakeDatetime.current = datetime(2025, 1, 1, 16, 1, tzinfo=pytz.UTC)
            ctx = multiprocessing.get_context("fork")
            processes = [
                ctx.Process(target=self._run, args=(service,)) for _ in range(6)
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(timeout=60)
            service.flush_all()

            files = storage.list_files(
                "test_provider/VehiclePosition/individual/", "*.parquet"
            )
            self.assertEqual(len(files), 1)
            df = pd.read_parquet(BytesIO(storage.read_bytes(files[0])))
            self.assertEqual(len(df), 6 * self._single_fetch_rows())


if __name__ == "__main__":
    unittest.main()
