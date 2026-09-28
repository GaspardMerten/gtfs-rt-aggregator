import unittest
from datetime import datetime, timezone

import pytz

from src.gtfs_rt_aggregator.utils.file_time import format_file_time, parse_file_time


class TestFileTime(unittest.TestCase):
    def test_utc_name_without_plus(self):
        dt = pytz.timezone("Europe/Amsterdam").localize(
            datetime(2026, 9, 28, 16, 0, 20)
        )
        name = format_file_time(dt)
        self.assertEqual(name, "2026-09-28_14-00-20Z")
        self.assertNotIn("+", name)
        self.assertEqual(parse_file_time(name), dt)

    def test_repeated_hour_gets_distinct_names(self):
        amsterdam = pytz.timezone("Europe/Amsterdam")
        first = amsterdam.localize(datetime(2026, 10, 25, 2, 30), is_dst=True)
        second = amsterdam.localize(datetime(2026, 10, 25, 2, 30), is_dst=False)
        self.assertNotEqual(format_file_time(first), format_file_time(second))

    def test_older_names(self):
        self.assertEqual(
            parse_file_time("2026-10-25_02-30-00+0100"),
            datetime(2026, 10, 25, 1, 30, tzinfo=timezone.utc),
        )
        self.assertEqual(
            parse_file_time("2026-10-25_02-30-00"), datetime(2026, 10, 25, 2, 30)
        )
        self.assertIsNone(parse_file_time("latest"))


if __name__ == "__main__":
    unittest.main()
