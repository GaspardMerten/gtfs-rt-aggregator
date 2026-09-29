"""Pieces of the runtime that are easier to test on their own."""

import os
import shutil
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    ProviderConfig,
    RuntimeConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.runtime.runtime import Runtime, Uploader
from src.gtfs_rt_aggregator.runtime.spool import Spool
from src.gtfs_rt_aggregator.storage.filesystem import FileSystemStorage


class TestSpoolRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.spool = Spool(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def _item(self):
        item = self.spool.new_item("f", datetime(2026, 9, 29, 8, tzinfo=timezone.utc))
        tmp = item.with_name(item.name + ".part")
        tmp.write_bytes(b"x")
        self.spool.commit_item(item, tmp, {"feed": "f", "attempt": 1})
        return item

    def test_claim_interrupted_between_renames(self):
        item = self._item()
        # The .pb moved, the process died before its sidecar
        target = self.spool.path("processing", "f", item.name)
        target.parent.mkdir(parents=True)
        os.replace(item, target)

        counts = self.spool.recover(max_attempts=3)
        self.assertEqual(counts["requeued"], 1)
        (back,) = self.spool.pending("f")
        self.assertEqual(self.spool.meta(back)["attempt"], 2)

    def test_orphan_sidecar_removed(self):
        item = self._item()
        item.unlink()
        self.spool.recover(max_attempts=3)
        self.assertEqual(list(self.spool.path("incoming", "f").iterdir()), [])


class TestUploader(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.spool = Spool(os.path.join(self.tmp, "spool"))
        self.storage = FileSystemStorage(os.path.join(self.tmp, "out"))
        self.uploader = Uploader(
            self.spool, {"global": self.storage}, threading.Event()
        )

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_file_rewritten_during_upload_is_kept(self):
        self.spool.put_ready("p", "p/_status/a.json", b"v1")
        original = self.storage.save_file

        def save_then_rewrite(local, path):
            result = original(local, path)
            self.spool.put_ready("p", "p/_status/a.json", b"v2")
            return result

        self.storage.save_file = save_then_rewrite
        self.uploader.upload_some()
        self.assertEqual(
            self.spool.ready_path("p", "p/_status/a.json").read_bytes(), b"v2"
        )

        self.storage.save_file = original
        self.uploader.upload_some()
        self.assertEqual(self.storage.read_bytes("p/_status/a.json"), b"v2")
        self.assertFalse(self.spool.ready_path("p", "p/_status/a.json").exists())

    def test_vanished_file_does_not_stop_uploads(self):
        self.spool.put_ready("p", "p/a.parquet", b"a")
        self.spool.put_ready("p", "p/b.parquet", b"b")
        original = self.storage.save_file

        def vanish(local, path):
            if path.endswith("a.parquet"):
                os.remove(local)
                raise FileNotFoundError(local)
            return original(local, path)

        self.storage.save_file = vanish
        self.assertEqual(self.uploader.upload_some(), 1)
        self.assertEqual(self.storage.read_bytes("p/b.parquet"), b"b")


class TestBackpressure(unittest.TestCase):
    def test_lowest_priority_first(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        apis = [
            ApiConfig(url=f"https://x.org/{p}", services=["Alert"], priority=p)
            for p in (0, 1, 2)
        ]
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={"base_directory": tmp}),
            providers=[ProviderConfig(name="p", realtime=apis)],
            runtime=RuntimeConfig(spool_dir=os.path.join(tmp, "spool"), spool_max_gb=1),
        )
        runtime = Runtime(config, {"global": MagicMock()})
        priority = {feed: api.priority for feed, (_, api) in runtime.feeds.items()}

        runtime.spool.size_bytes = lambda: 2 * 1024**3  # full
        runtime._check_spool_size()
        self.assertEqual({priority[f] for f in runtime._paused}, {0})
        runtime._check_spool_size()
        self.assertEqual({priority[f] for f in runtime._paused}, {0, 1})
        runtime._check_spool_size()
        runtime._check_spool_size()
        self.assertEqual({priority[f] for f in runtime._paused}, {0, 1, 2})

        runtime.spool.size_bytes = lambda: int(0.5 * 1024**3)
        runtime._check_spool_size()
        self.assertEqual(runtime._paused, set())


if __name__ == "__main__":
    unittest.main()
