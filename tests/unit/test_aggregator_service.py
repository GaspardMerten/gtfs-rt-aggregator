import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import pytz

from src.gtfs_rt_aggregator.aggregator.service import AggregatorService
from src.gtfs_rt_aggregator.config.models import (
    GtfsRtConfig,
    ProviderConfig,
    ApiConfig,
    StorageConfig,
)
from tests.mocks import MockStorageInterface, create_mock_parquet_files


class TestAggregatorService(unittest.TestCase):
    """Test cases for the AggregatorService."""

    def setUp(self):
        """Set up test environment."""
        # Create a mock storage and configuration
        self.storage = MockStorageInterface()

        # Mock configuration
        self.provider_name = "test_provider"
        self.service_type = "VehiclePosition"
        self.frequency_minutes = 15
        self.timezone_str = "UTC"
        self.timezone = pytz.timezone(self.timezone_str)

        self.config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={}),
            providers=[
                ProviderConfig(
                    name=self.provider_name,
                    timezone=self.timezone_str,
                    apis=[
                        ApiConfig(
                            url="http://localhost:8788/vehicle_positions",
                            services=[self.service_type],
                            refresh_seconds=60,
                            frequency_minutes=self.frequency_minutes,
                            check_interval_seconds=300,
                        )
                    ],
                )
            ],
        )

        # Create the aggregator service with our mock storage
        # Note: we need to add both provider-specific and global storage
        self.aggregator = AggregatorService(
            self.config,
            {
                self.provider_name: self.storage,
                "global": self.storage,  # Add global storage as fallback
            },
        )

    def test_group_files_by_time(self):
        """Test _group_files_by_time method."""
        # Create test files with timestamps spanning 1 hour
        base_time = datetime.now(pytz.UTC).replace(minute=0, second=0, microsecond=0)

        # Create filenames representing different times with the correct format (YYYY-MM-DD_HH-MM-SS)
        filenames = []
        for i in range(60):  # 1 hour with 1-minute intervals
            time = base_time + timedelta(minutes=i)
            time_str = time.strftime(
                "%Y-%m-%d_%H-%M-%S"
            )  # Use dashes instead of no separator
            filenames.append(
                f"{self.provider_name}/{self.service_type}/individual/individual_{time_str}.parquet"
            )

        # Test grouping with 15-minute intervals
        groups = self.aggregator._group_files_by_time(
            filenames, self.frequency_minutes, self.timezone
        )

        # We should have 4 groups (15-minute intervals in an hour)
        self.assertEqual(len(groups), 4)

        # Each group should have 15 files
        for group_time, files in groups.items():
            self.assertEqual(len(files), 15)

    def test_aggregate_service_type(self):
        """Test _aggregate_service_type method."""
        # First, create a few mock files
        base_time = datetime.now(pytz.UTC).replace(minute=0, second=0, microsecond=0)
        base_time_str = base_time.isoformat()
        create_mock_parquet_files(
            self.storage, self.provider_name, self.service_type, 5, base_time_str
        )

        # Mock _group_files_by_time and _aggregate_files to avoid actual processing
        with (
            patch.object(self.aggregator, "_group_files_by_time") as mock_group,
            patch.object(self.aggregator, "_aggregate_files") as mock_aggregate,
        ):

            # Set up the mock to return a dictionary with one timestamp and 5 files
            fake_timestamp = base_time
            fake_files = [f"file{i}.parquet" for i in range(5)]
            mock_group.return_value = {fake_timestamp: fake_files}

            # Run aggregation
            self.aggregator._aggregate_service_type(
                self.provider_name,
                self.service_type,
                self.frequency_minutes,
                self.timezone,
            )

            # Verify the mocks were called
            # Use ANY for some arguments that might be generated internally
            mock_group.assert_called_once()
            self.assertTrue(mock_aggregate.called)

    def test_run_once(self):
        """Test run_once method with mock data."""
        # Mock the _aggregate_service_type method to avoid the actual aggregation
        with patch.object(self.aggregator, "_aggregate_service_type") as mock_aggregate:
            # Run the aggregation job once
            self.aggregator.run_once(
                provider_name=self.provider_name,
                service_types=[self.service_type],
                frequency_minutes=self.frequency_minutes,
                timezone=self.timezone_str,
            )

            # Verify that _aggregate_service_type was called with the right provider and service type
            # Use ANY for arguments that might vary or be internally generated
            mock_aggregate.assert_called_once()
            args, kwargs = mock_aggregate.call_args
            self.assertEqual(kwargs.get("provider_name"), self.provider_name)
            self.assertEqual(kwargs.get("service_type"), self.service_type)
            self.assertEqual(kwargs.get("frequency_minutes"), self.frequency_minutes)

    def test_extract_datetime_with_and_without_offset(self):
        amsterdam = pytz.timezone("Europe/Amsterdam")
        extract = self.aggregator._extract_datetime_from_filename
        expected = amsterdam.localize(datetime(2026, 9, 28, 16, 0, 20))

        utc = extract("p/VP/individual/2026-09-28_14-00-20Z.parquet", amsterdam)
        self.assertEqual(utc, expected)
        self.assertEqual(utc.hour, 16)
        # Names from 0.3.0: local time with offset
        self.assertEqual(
            extract("p/VP/individual/2026-09-28_16-00-20+0200.parquet", amsterdam),
            expected,
        )
        # Written in another offset: same instant
        converted = extract(
            "p/VP/individual/2026-09-28_14-00-20+0000.parquet", amsterdam
        )
        self.assertEqual(converted, expected)
        self.assertEqual(converted.hour, 16)
        # Before 0.3.0: naive local time
        self.assertEqual(
            extract("p/VP/individual/2026-09-28_16-00-20.parquet", amsterdam),
            datetime(2026, 9, 28, 16, 0, 20),
        )
        self.assertIsNone(extract("p/VP/individual/not-a-date.parquet", amsterdam))

    def test_repeated_hour_when_clocks_go_back(self):
        # 2026-10-25 in Amsterdam: 02:00-03:00 happens first at +0200, then at +0100
        amsterdam = pytz.timezone("Europe/Amsterdam")
        first, second, after = (
            "p/VP/individual/2026-10-25_00-30-00Z.parquet",
            "p/VP/individual/2026-10-25_01-30-00Z.parquet",
            "p/VP/individual/2026-10-25_02-00-00Z.parquet",
        )
        groups = self.aggregator._group_files_by_time(
            [first, second, after], 60, amsterdam
        )

        # Three distinct hours; the two passes of 02:00 are written to the same
        # output file, which the aggregator extends instead of replacing
        self.assertEqual(
            [groups[group] for group in sorted(groups)], [[first], [second], [after]]
        )
        self.assertEqual(
            [group.strftime("%H:%M%z") for group in sorted(groups)],
            ["02:00+0200", "02:00+0100", "03:00+0100"],
        )

    def _simulate(self, timezone, start_utc, hours, frequency, step_minutes=5):
        """Fetch every step_minutes and aggregate after each fetch; return
        (fetches, rows in aggregated files, individual files left)."""
        import io

        import pyarrow as pa
        import pyarrow.parquet as pq

        from src.gtfs_rt_aggregator.utils.file_time import format_file_time

        storage = MockStorageInterface()
        aggregator = AggregatorService(self.config, {"global": storage})
        t = start_utc
        fetches = 0
        while t < start_utc + timedelta(hours=hours):
            buffer = io.BytesIO()
            pq.write_table(pa.table({"utc": [t.isoformat()]}), buffer)
            name = format_file_time(t)
            storage.save_bytes(buffer.getvalue(), f"p/VP/individual/{name}.parquet")
            fetches += 1
            aggregator.run_once("p", ["VP"], frequency, timezone)
            t += timedelta(minutes=step_minutes)

        rows = []
        left = 0
        for path in storage.list_paths("p/VP/"):
            table = pq.read_table(io.BytesIO(storage.get_bytes(path)))
            if "/individual/" in path:
                left += table.num_rows
            else:
                rows += table.column("utc").to_pylist()
        self.assertEqual(len(rows), len(set(rows)), "duplicated rows")
        return fetches, len(rows), left

    def test_no_loss_around_clock_changes(self):
        cases = [
            # clocks go back
            ("Europe/Amsterdam", datetime(2026, 10, 24, 23, tzinfo=pytz.UTC), 4),
            ("Europe/London", datetime(2026, 10, 24, 23, tzinfo=pytz.UTC), 4),
            # clocks go forward
            ("Europe/Amsterdam", datetime(2026, 3, 28, 23, tzinfo=pytz.UTC), 4),
        ]
        for timezone, start, hours in cases:
            for frequency in (15, 60):
                with self.subTest(timezone=timezone, start=start, frequency=frequency):
                    fetches, aggregated, left = self._simulate(
                        timezone, start, hours, frequency
                    )
                    self.assertEqual(aggregated + left, fetches)
                    # Only the last, unfinished period may still be waiting
                    self.assertLessEqual(left, frequency // 5)

    def test_daily_groups_on_clock_change_days(self):
        for start in (
            datetime(2026, 10, 24, 12, tzinfo=pytz.UTC),
            datetime(2026, 3, 28, 12, tzinfo=pytz.UTC),
        ):
            with self.subTest(start=start):
                fetches, aggregated, left = self._simulate(
                    "Europe/Amsterdam", start, 60, 1440, step_minutes=60
                )
                self.assertEqual(aggregated + left, fetches)
                self.assertLessEqual(left, 24)

    def test_rounding_keeps_the_right_offset(self):
        amsterdam = pytz.timezone("Europe/Amsterdam")
        # 16:00 on the day clocks go back is +0100, but that day's midnight is +0200
        dt = amsterdam.localize(datetime(2026, 10, 25, 16, 0))
        midnight = AggregatorService._get_rounded_time(dt, 1440)
        self.assertEqual(midnight, amsterdam.localize(datetime(2026, 10, 25, 0, 0)))
        self.assertEqual(midnight.utcoffset(), timedelta(hours=2))

    def test_get_scheduling(self):
        """Test get_scheduling method."""
        # Get the scheduling
        schedules = self.aggregator.get_scheduling()

        # Check that we have at least one schedule
        self.assertTrue(len(schedules) > 0)

        # Each schedule should be a tuple with the expected elements
        for schedule in schedules:
            self.assertIsInstance(schedule, tuple)

            # First item should be check_interval_seconds
            self.assertIsInstance(schedule[0], int)

            # Second item should be the run_once method
            self.assertEqual(schedule[1], self.aggregator.run_once)

            # The third item should be a job name (string)
            self.assertIsInstance(schedule[2], str)
            self.assertIn(self.provider_name, schedule[2])

            # Fourth item should be kwargs dict
            self.assertIsInstance(schedule[3], dict)

            # Check the kwargs
            kwargs = schedule[3]
            self.assertIn("provider_name", kwargs)
            self.assertEqual(kwargs["provider_name"], self.provider_name)
            self.assertIn("service_types", kwargs)
            self.assertIn("frequency_minutes", kwargs)
            self.assertIn("timezone", kwargs)


if __name__ == "__main__":
    unittest.main()
