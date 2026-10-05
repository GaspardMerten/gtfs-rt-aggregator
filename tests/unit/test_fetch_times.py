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


def _fetch(entities, fetch_time, *more):
    """
    An individual file as the worker writes it: rows and fetch time. With
    more (entities, fetch_time) pairs: a window file holding several fetches.
    """
    fetches = [(entities, fetch_time)] + list(zip(more[::2], more[1::2]))
    rows = [(e, t) for entities, t in fetches for e in entities]
    table = pa.table(
        {
            "entityId": pa.array([e for e, _ in rows], pa.string()),
            "contentHash": pa.array(["h"] * len(rows), pa.string()),
            "fetchTime": pa.array([t for _, t in rows], TIMESTAMP),
            "feedId": pa.array([FEED] * len(rows), pa.string()),
        }
    )
    times = {}
    for _, t in fetches:
        fetch_times.merge(times, fetch_times.of_fetch(FEED, t))
    table = fetch_times.with_times(table, times)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def _unchanged(hour, minutes):
    """A file with no rows recording unchanged fetches, as the worker writes it."""
    table = pa.table(
        {
            "entityId": pa.array([], pa.string()),
            "contentHash": pa.array([], pa.string()),
            "fetchTime": pa.array([], TIMESTAMP),
            "feedId": pa.array([], pa.string()),
        }
    )
    state = {}
    fetch_times.note_unchanged(
        state, [(hour + timedelta(minutes=m)).isoformat() for m in minutes]
    )
    table = fetch_times.with_times(
        table, {}, fetch_times.pending_unchanged(state, FEED)
    )
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


class TestEncoding(unittest.TestCase):
    def test_round_trip(self):
        times = {"a": {3, 1, 2_000_000}, None: {5}}
        self.assertEqual(fetch_times.decode(fetch_times.encode(times)), times)
        self.assertIsNone(fetch_times.decode({}))

    def test_unchanged_fetches_apart(self):
        table = fetch_times.with_times(pa.table({"a": [1]}), {"f": {1}}, {"f": {2, 3}})
        metadata = table.schema.metadata
        self.assertEqual(fetch_times.decode(metadata), {"f": {1}})
        self.assertEqual(
            fetch_times.decode(metadata, fetch_times.UNCHANGED_TIMES_METADATA),
            {"f": {2, 3}},
        )
        # Rewritten without: not carried over
        table = fetch_times.with_times(table, {"f": {1}})
        self.assertIsNone(
            fetch_times.decode(
                table.schema.metadata, fetch_times.UNCHANGED_TIMES_METADATA
            )
        )


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

    def test_gap_from_a_later_file(self):
        # A window file holds the fetches of minutes 0 and 2; a slow fetch of
        # minute 1, without "a", arrived later in its own file
        self._put(
            None,
            0,
            _fetch(["a"], self.hour, ["a"], self.hour + timedelta(minutes=2)),
        )
        self._put([], 1)
        self._run()
        self.assertEqual(self._runs(self._output()), [("a", 0, 0), ("a", 2, 2)])

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

    def test_outage_ends_run(self):
        # Not read for 40 minutes (an outage): "a" is not known to have stayed
        self._put(["a"], 0)
        self._put(["a"], 40)
        self._run()
        self.assertEqual(self._runs(self._output()), [("a", 0, 0), ("a", 40, 40)])

    def test_unchanged_fetches_bridge_the_wait(self):
        # Read every minute in between, unchanged: a file with no rows records
        # those fetches (see worker.process_item)
        self._put(["a"], 0)
        self._put(None, 20, _unchanged(self.hour, range(1, 20)))
        self._put(None, 39, _unchanged(self.hour, range(20, 40)))
        self._put(["a"], 40)
        self._run()
        output = self._output()
        self.assertEqual(self._runs(output), [("a", 0, 40)])
        unchanged = fetch_times.decode(
            output.schema.metadata, fetch_times.UNCHANGED_TIMES_METADATA
        )
        self.assertEqual(len(unchanged[FEED]), 39)

        # Kept when the day is compacted
        self.aggregator.compact_once(
            "p", [SERVICE], "UTC", deduplicate=True, days_back=None
        )
        folder = f"provider=p/service={SERVICE}/date={self.hour:%Y-%m-%d}"
        day = pq.read_table(io.BytesIO(self.storage.get_bytes(f"{folder}/day.parquet")))
        self.assertEqual(self._runs(day), [("a", 0, 40)])

    def test_only_empty_fetches(self):
        self._put([], 0)
        self._run()
        table = self._output()
        self.assertEqual(table.num_rows, 0)
        self.assertEqual(len(fetch_times.decode(table.schema.metadata)[FEED]), 1)


class TestWorthStoring(unittest.TestCase):
    def test_empty_service_stored_once(self):
        state = {}
        rows = pa.table({"entityId": ["a"]})
        empty = rows.slice(0, 0)
        stored = []
        for tables in (
            {"Alert": rows, "TripUpdate": empty},
            {"Alert": empty, "TripUpdate": empty},
            {"Alert": empty, "TripUpdate": empty},
            {"Alert": rows, "TripUpdate": empty},
        ):
            stored.append(sorted(fetch_times.worth_storing(tables, state)))
        # The first empty fetch after rows ends the runs; the next ones add nothing
        self.assertEqual(
            stored,
            [["Alert", "TripUpdate"], ["Alert"], [], ["Alert"]],
        )


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
