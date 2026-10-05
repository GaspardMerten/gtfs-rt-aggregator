"""Worker tasks, run in this process."""

import io
import logging
import signal
import shutil
import tarfile
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    ProviderConfig,
    RuntimeConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.aggregator import fetch_times
from src.gtfs_rt_aggregator.runtime import worker
from src.gtfs_rt_aggregator.runtime.core import feed_hash, feed_id
from src.gtfs_rt_aggregator.runtime.spool import Spool

DATA = Path(__file__).parent.parent / "data"


class TestWorkerTasks(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.api = ApiConfig(
            url="https://example.org/vp.pb",
            services=["VehiclePosition"],
            accumulate_minutes=15,
            skip_unchanged=False,
        )
        self.config = GtfsRtConfig(
            storage=StorageConfig(
                type="filesystem", params={"base_directory": str(self.tmp / "out")}
            ),
            providers=[
                ProviderConfig(
                    name="p", timezone="Europe/Brussels", realtime=[self.api]
                )
            ],
            runtime=RuntimeConfig(spool_dir=str(self.tmp / "spool")),
        )
        # init_worker makes the process ignore them
        self._signals = {
            number: signal.getsignal(number)
            for number in (signal.SIGINT, signal.SIGTERM)
        }
        worker.init_worker(self.config, str(self.tmp / "spool"), logging.INFO)
        self.spool = Spool(str(self.tmp / "spool"))
        self.feed = feed_id("p", self.api)

    def tearDown(self):
        worker._CTX = None
        for number, handler in self._signals.items():
            signal.signal(number, handler)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _item(self, when: datetime, **meta) -> Path:
        item = self.spool.new_item(self.feed, when)
        tmp = item.with_name(item.name + ".part")
        tmp.write_bytes((DATA / "vehicle_positions.pb").read_bytes())
        self.spool.commit_item(
            item,
            tmp,
            {"feed": self.feed, "fetch_time": when.isoformat(), "attempt": 1, **meta},
        )
        return self.spool.claim(item)

    def test_unchanged_fetches_recorded(self):
        self.api.skip_unchanged = True
        first = datetime(2026, 9, 29, 8, 1, tzinfo=timezone.utc)
        self.assertEqual(len(worker.process_item(str(self._item(first)))["written"]), 1)
        # Same entities; the runtime skipped an identical download at 08:02
        result = worker.process_item(
            str(
                self._item(
                    first.replace(minute=3),
                    unchanged_fetch_times=[first.replace(minute=2).isoformat()],
                )
            )
        )
        self.assertTrue(result["summary"]["unchanged"])
        self.assertEqual(result["written"], [])
        # Ten minutes after the oldest one: written in a part with no rows
        result = worker.process_item(str(self._item(first.replace(minute=12))))
        self.assertEqual(len(result["written"]), 1)
        self.assertNotIn("unchanged", self.spool.state(self.feed))

        (window,) = self.spool.path("windows").glob("*/*/*")
        worker.close_window(str(window))
        (ready,) = self.spool.path("ready").rglob("*.parquet")
        table = pq.read_table(ready)
        self.assertEqual(table.num_rows, 3549)
        feed = feed_hash(self.api)
        self.assertEqual(len(fetch_times.decode(table.schema.metadata)[feed]), 1)
        unchanged = fetch_times.decode(
            table.schema.metadata, fetch_times.UNCHANGED_TIMES_METADATA
        )[feed]
        self.assertEqual(
            sorted(datetime.fromtimestamp(t / 1e6, timezone.utc).minute for t in unchanged),
            [2, 3, 12],
        )

    def test_accumulation_window(self):
        first = datetime(2026, 9, 29, 8, 1, tzinfo=timezone.utc)
        for minute in (1, 5):
            item = self._item(first.replace(minute=minute))
            result = worker.process_item(str(item))
            self.assertEqual(result["summary"]["kept_count"], 3549)
            self.assertIn("peak_memory_mb", result)
            self.spool.done(item, archive=False)

        (window,) = self.spool.path("windows").glob("*/*/*")
        self.assertEqual(window.name, "2026-09-29_08-00-00Z")  # 10:00 in Brussels
        self.assertEqual(len(list(window.glob("part-*.parquet"))), 2)

        result = worker.close_window(str(window))
        self.assertEqual(result["parts"], 2)
        self.assertFalse(window.exists())
        (ready,) = self.spool.path("ready").rglob("*.parquet")
        self.assertEqual(
            ready.name, f"2026-09-29_08-01-00Z-{feed_hash(self.api)}.parquet"
        )
        table = pq.read_table(ready)
        self.assertEqual(table.num_rows, 2 * 3549)
        # Both fetch times are recorded (see fetch_times.py)
        times = fetch_times.decode(table.schema.metadata)
        self.assertEqual(len(times[feed_hash(self.api)]), 2)

    def test_live_snapshot(self):
        worker._CTX = None
        self.config.output.live_snapshot = True
        worker.init_worker(self.config, str(self.tmp / "spool"), logging.INFO)
        first = datetime(2026, 9, 29, 8, 1, tzinfo=timezone.utc)
        live = self.tmp / "out" / "p" / "_live" / "VehiclePosition" / f"VehiclePosition-{feed_hash(self.api)}.parquet"
        worker.process_item(str(self._item(first)))
        self.assertEqual(pq.read_table(live).num_rows, 3549)
        written = live.stat().st_mtime_ns
        # Within live_seconds of the previous snapshot: not rewritten
        worker.process_item(str(self._item(first.replace(minute=2))))
        self.assertEqual(live.stat().st_mtime_ns, written)

    def test_feeds_of_one_service_fetched_in_the_same_second(self):
        # A second feed of the same provider and service (e.g. another operator)
        other = ApiConfig(
            url="https://example.org/other/vp.pb",
            services=["VehiclePosition"],
            skip_unchanged=False,
        )
        self.config.providers[0].realtime.append(other)
        worker.init_worker(self.config, str(self.tmp / "spool"), logging.INFO)
        when = datetime(2026, 9, 29, 8, 1, tzinfo=timezone.utc)
        worker.process_item(str(self._item(when)))
        (window,) = self.spool.path("windows").glob("*/*/*")
        worker.close_window(str(window))

        self.feed = feed_id("p", other)
        worker.process_item(str(self._item(when)))
        ready = sorted(self.spool.path("ready").rglob("*.parquet"))
        self.assertEqual(len(ready), 2)
        feeds = {pq.read_table(r)["feedId"][0].as_py() for r in ready}
        self.assertEqual(feeds, {feed_hash(self.api), feed_hash(other)})

        # Both land in the same aggregation period
        from src.gtfs_rt_aggregator.aggregator.service import AggregatorService

        service = AggregatorService(self.config, {})
        times = {service._extract_datetime_from_filename(r.name) for r in ready}
        self.assertEqual(len(times), 1)

    def test_waits_for_the_static_version_of_its_filter(self):
        from datetime import timedelta

        from src.gtfs_rt_aggregator.config.models import FilterConfig, StaticConfig
        from src.gtfs_rt_aggregator.runtime.core import StaticNotReady

        self.api.filter = FilterConfig(route_types=[2])
        self.config.providers[0].static.append(
            StaticConfig(url="https://example.org/gtfs.zip")
        )
        worker.init_worker(self.config, str(self.tmp / "spool"), logging.INFO)
        # No static version stored yet: nothing written, the fetch waits
        with self.assertRaises(StaticNotReady):
            worker.process_item(str(self._item(datetime.now(timezone.utc))))
        self.assertEqual(list(self.spool.path("windows").rglob("*.parquet")), [])
        # However long it waited: never dropped, never stored unfiltered
        old = datetime.now(timezone.utc) - timedelta(hours=30)
        with self.assertRaises(StaticNotReady):
            worker.process_item(str(self._item(old)))
        self.assertEqual(list(self.spool.path("windows").rglob("*.parquet")), [])

    def test_fetch_after_close_gets_its_own_window(self):
        first = datetime(2026, 9, 29, 8, 1, tzinfo=timezone.utc)
        worker.process_item(str(self._item(first)))
        (window,) = self.spool.path("windows").glob("*/*/*")
        worker.close_window(str(window))
        self.assertFalse(window.exists())

        # A slow fetch of the same window, processed after the close
        worker.process_item(str(self._item(first.replace(minute=2))))
        self.assertTrue((window / "window.json").exists())
        self.assertEqual(len(list(window.glob("part-*.parquet"))), 1)
        worker.close_window(str(window))
        self.assertEqual(len(list(self.spool.path("ready").rglob("*.parquet"))), 2)

    def test_raw_bundle(self):
        item = self._item(datetime(2026, 9, 29, 8, 1, tzinfo=timezone.utc))
        worker.process_item(str(item))
        self.spool.done(item, archive=True)
        (hour,) = self.spool.path("raw").glob("*/*/*")
        self.assertEqual((hour.parent.name, hour.name), ("20260929", "08"))

        result = worker.bundle_raw(
            str(hour), "p", "raw/provider=p/feed=x/date=2026-09-29/08-1.tar.zst"
        )
        self.assertEqual(result["files"], 2)  # .pb and .json
        self.assertFalse(hour.exists())
        bundle = self.spool.ready_path(
            "p", "raw/provider=p/feed=x/date=2026-09-29/08-1.tar.zst"
        )
        with pa.input_stream(str(bundle), compression="zstd") as stream:
            with tarfile.open(fileobj=io.BytesIO(stream.read())) as tar:
                names = sorted(tar.getnames())
        self.assertEqual(len(names), 2)
        self.assertTrue(names[0].endswith(".json") and names[1].endswith(".pb"))

    def test_raw_retention(self):
        from datetime import timedelta

        storage = worker._ctx().storage("p")
        today = datetime.now(timezone.utc).date()
        paths = {
            back: f"raw/provider=p/feed=x/date={today - timedelta(days=back)}/08-1.tar.zst"
            for back in (0, 3, 4, 30)
        }
        for path in paths.values():
            storage.save_bytes(b"x", path)
        storage.save_bytes(b"x", "raw/provider=other/feed=x/date=2020-01-01/08-1.tar.zst")
        result = worker.prune_raw("p", "raw", 3)
        self.assertEqual(result["deleted"], 2)
        self.assertEqual(
            sorted(storage.walk_files("raw/provider=p/")), sorted([paths[0], paths[3]])
        )
        # Another provider's bundles are its own task's
        self.assertTrue(
            storage.file_exists("raw/provider=other/feed=x/date=2020-01-01/08-1.tar.zst")
        )


if __name__ == "__main__":
    unittest.main()
