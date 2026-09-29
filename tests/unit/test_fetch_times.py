"""Fetch times kept in file metadata, and aggregation that survives failures."""

import io
import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

from src.gtfs_rt_aggregator.aggregator import fetch_times
from src.gtfs_rt_aggregator.aggregator.compaction import compact_files, write_sorted
from src.gtfs_rt_aggregator.aggregator.service import (
    SOURCES_METADATA,
    AggregatorService,
)
from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    OutputConfig,
    ProviderConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.utils.file_time import format_file_time
from tests.mocks import MockStorageInterface

TIMESTAMP = pa.timestamp("us", tz="UTC")
SERVICE = "VehiclePosition"
FEED = "abcd0123"


def _fetch(entities, fetch_time):
    """An individual file as the worker writes it: rows and fetch time."""
    table = pa.table(
        {
            "entityId": pa.array(entities, pa.string()),
            "contentHash": pa.array(["h"] * len(entities), pa.string()),
            "fetchTime": pa.array([fetch_time] * len(entities), TIMESTAMP),
            "feedId": pa.array([FEED] * len(entities), pa.string()),
        }
    )
    table = fetch_times.with_times(table, fetch_times.of_fetch(FEED, fetch_time))
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


class TestEncoding(unittest.TestCase):
    def test_round_trip(self):
        times = {"a": {3, 1, 2_000_000}, None: {5}}
        self.assertEqual(fetch_times.decode(fetch_times.encode(times)), times)
        self.assertIsNone(fetch_times.decode({}))


class TestAggregation(unittest.TestCase):
    def setUp(self):
        self.storage = MockStorageInterface()
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem"),
            providers=[
                ProviderConfig(
                    name="p",
                    timezone="UTC",
                    realtime=[ApiConfig(url="u", services=[SERVICE], deduplicate=True)],
                )
            ],
            output=OutputConfig(compact_daily=True),
        )
        self.aggregator = AggregatorService(config, {"global": self.storage})
        # An hour of a finished day
        self.hour = (datetime.now(timezone.utc) - timedelta(hours=26)).replace(
            minute=0, second=0, microsecond=0
        )

    def _put(self, entities, minute, data=None):
        fetch_time = self.hour + timedelta(minutes=minute)
        path = f"p/{SERVICE}/individual/{format_file_time(fetch_time, FEED)}.parquet"
        self.storage.save_bytes(data or _fetch(entities, fetch_time), path)
        return path

    def _run(self, deduplicate=True):
        self.aggregator.run_once("p", [SERVICE], 60, "UTC", deduplicate=deduplicate)

    def _output(self):
        path = self.aggregator.output_path(
            "p", SERVICE, self.hour, self.hour + timedelta(hours=1)
        )
        return pq.read_table(io.BytesIO(self.storage.get_bytes(path)))

    def _runs(self, table):
        return sorted(
            (
                row["entityId"],
                row["firstSeen"].minute,
                row["lastSeen"].minute,
            )
            for row in table.to_pylist()
        )

    def test_gap_kept_when_merged_again(self):
        # "a" is missing from the fetch at minute 1 (a fetch with no entity)
        self._put(["a"], 0)
        self._put([], 1)
        self._put(["a"], 2)
        self._run()
        self.assertEqual(self._runs(self._output()), [("a", 0, 0), ("a", 2, 2)])

        # A late fetch is merged into the file: the gap at minute 1 is known
        # from the file's metadata, not from its rows
        self._put(["a"], 3)
        self._run()
        self.assertEqual(self._runs(self._output()), [("a", 0, 0), ("a", 2, 3)])

        # And again when the day is compacted
        self.aggregator.compact_once(
            "p", [SERVICE], "UTC", deduplicate=True, days_back=None
        )
        folder = f"provider=p/service={SERVICE}/date={self.hour:%Y-%m-%d}"
        day = pq.read_table(io.BytesIO(self.storage.get_bytes(f"{folder}/day.parquet")))
        self.assertEqual(self._runs(day), [("a", 0, 0), ("a", 2, 3)])

    def test_stopped_before_deleting(self):
        # The process stopped after writing the aggregated file, before
        # deleting the individual files: they are not added twice
        data = _fetch(["a", "b"], self.hour)
        path = self._put(None, 0, data)
        self._run(deduplicate=False)
        self.assertEqual(self._output().num_rows, 2)
        self.storage.save_bytes(data, path)
        self._run(deduplicate=False)
        self.assertEqual(self._output().num_rows, 2)
        self.assertFalse(self.storage.file_exists(path))
        sources = json.loads(self._output().schema.metadata[SOURCES_METADATA])
        self.assertEqual(len(sources), 1)

    def test_unreadable_file_moved_aside(self):
        bad = self._put(None, 0, b"not parquet")
        self._put(["a"], 1)
        self._run(deduplicate=False)
        self.assertEqual(self._output()["entityId"].to_pylist(), ["a"])
        self.assertFalse(self.storage.file_exists(bad))
        self.assertTrue(self.storage.file_exists(bad.replace("individual", "error")))

    def test_only_empty_fetches(self):
        self._put([], 0)
        self._run()
        table = self._output()
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(len(fetch_times.decode(table.schema.metadata)[FEED]), 1)


class TestCompactionCarry(unittest.TestCase):
    def test_rows_without_entity_id_kept(self):
        times = [datetime(2026, 1, 1, 0, m, tzinfo=timezone.utc) for m in range(3)]
        table = pa.table(
            {
                "entityId": pa.array(["a", None, None], pa.string()),
                "contentHash": pa.array(["h", "x", "y"], pa.string()),
                "fetchTime": pa.array(times, TIMESTAMP),
                "firstSeen": pa.array(times, TIMESTAMP),
                "lastSeen": pa.array(times, TIMESTAMP),
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "in.parquet")
            write_sorted(table, ["entityId", "firstSeen"], source)
            output = os.path.join(tmp, "out.parquet")
            rows = compact_files(
                [source], ["entityId", "firstSeen"], output, deduplicate_rows=True
            )
            self.assertEqual(rows, 3)


if __name__ == "__main__":
    unittest.main()
