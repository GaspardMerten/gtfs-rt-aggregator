"""Worker tasks, run in this process."""

import io
import logging
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
from src.gtfs_rt_aggregator.runtime import worker
from src.gtfs_rt_aggregator.runtime.core import feed_id
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
        worker.init_worker(self.config, str(self.tmp / "spool"), logging.INFO)
        self.spool = Spool(str(self.tmp / "spool"))
        self.feed = feed_id("p", self.api)

    def tearDown(self):
        worker._CTX = None
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _item(self, when: datetime) -> Path:
        item = self.spool.new_item(self.feed, when)
        tmp = item.with_name(item.name + ".part")
        tmp.write_bytes((DATA / "vehicle_positions.pb").read_bytes())
        self.spool.commit_item(
            item, tmp, {"feed": self.feed, "fetch_time": when.isoformat(), "attempt": 1}
        )
        return self.spool.claim(item)

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
        self.assertEqual(ready.name, "2026-09-29_08-01-00Z.parquet")
        self.assertEqual(pq.read_table(ready).num_rows, 2 * 3549)

    def test_late_part_kept_for_next_close(self):
        item = self._item(datetime(2026, 9, 29, 8, 1, tzinfo=timezone.utc))
        worker.process_item(str(item))
        (window,) = self.spool.path("windows").glob("*/*/*")
        parts = list(window.glob("part-*.parquet"))
        # A part arriving while the window closes stays for the next close
        original_glob = Path.glob

        def glob_then_add(path, pattern):
            found = list(original_glob(path, pattern))
            if (
                path == window
                and pattern == "part-*.parquet"
                and not (window / "part-late.parquet").exists()
            ):
                shutil.copy(parts[0], window / "part-late.parquet")
            return iter(found)

        from unittest.mock import patch

        with patch.object(Path, "glob", glob_then_add):
            worker.close_window(str(window))
        self.assertTrue((window / "part-late.parquet").exists())
        self.assertTrue((window / "window.json").exists())

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


if __name__ == "__main__":
    unittest.main()
