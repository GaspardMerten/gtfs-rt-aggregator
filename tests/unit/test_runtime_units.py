"""Pieces of the runtime that are easier to test on their own."""

import os
import shutil
import tempfile
import threading
import time
import unittest
from datetime import datetime, timezone
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
        (back,) = self.spool.queued_items()["f"]
        self.assertEqual(self.spool.meta(back)["attempt"], 2)

    def test_clean_shutdown_counts_no_attempt(self):
        self.spool.claim(self._item())
        self.spool.mark_clean_shutdown()
        self.spool.recover(max_attempts=3)
        (back,) = self.spool.queued_items()["f"]
        meta = self.spool.meta(back)
        self.assertEqual(meta["attempt"], 1)
        self.assertNotIn("suspect", meta)
        # The marker is used once
        self.spool.claim(back)
        self.spool.recover(max_attempts=3)
        (back,) = self.spool.queued_items()["f"]
        self.assertEqual(self.spool.meta(back)["attempt"], 2)

    def test_failure_on_its_own_clears_suspect(self):
        item = self.spool.claim(self._item())
        self.spool.release(item, "worker died", 3, count_attempt=False)
        (back,) = self.spool.queued_items()["f"]
        self.assertTrue(self.spool.meta(back)["suspect"])
        self.spool.release(self.spool.claim(back), "boom", 3)
        (back,) = self.spool.queued_items()["f"]
        self.assertNotIn("suspect", self.spool.meta(back))

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


class TestHeavyTasks(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={"base_directory": tmp}),
            providers=[
                ProviderConfig(
                    name="p",
                    realtime=[
                        # Two feeds of one service: one aggregation job
                        ApiConfig(url="https://x.org/a", services=["TripUpdate"]),
                        ApiConfig(url="https://x.org/b", services=["TripUpdate"]),
                    ],
                )
            ],
            runtime=RuntimeConfig(spool_dir=os.path.join(tmp, "spool"), heavy_slots=2),
        )
        self.runtime = Runtime(config, {"global": MagicMock()})

    def test_one_job_per_service(self):
        self.runtime._schedule_jobs()
        names = [job.name for job in self.runtime._jobs]
        self.assertEqual(
            [n for n in names if n.startswith("Aggregator")],
            ["Aggregator - p - TripUpdate"],
        )

    def test_same_lock_never_runs_at_once(self):
        runtime = self.runtime
        runtime._normal_pool = MagicMock()
        submitted = []
        runtime._submit = lambda task, *a, **k: (
            submitted.append(task.key),
            runtime._in_flight.__setitem__(object(), task),
        )
        runtime._queue_heavy("aggregate", "agg", {}, lock="p/TripUpdate")
        runtime._queue_heavy("compact", "compact", {}, lock="p/TripUpdate")
        runtime._queue_heavy("aggregate", "other", {}, lock="p/Alert")
        runtime._dispatch()
        # Two slots: the compaction waits for the aggregation of its service
        self.assertEqual(submitted, ["agg", "other"])
        self.assertEqual([t.key for t in runtime._heavy_queue], ["compact"])

    def test_spool_used_by_one_pipeline_only(self):
        self.runtime._lock_spool()
        self.addCleanup(self.runtime._spool_lock.close)
        other = Runtime(self.runtime.config, {"global": MagicMock()})
        with self.assertRaisesRegex(RuntimeError, "Another pipeline"):
            other._lock_spool()

    def test_refused_pipeline_leaves_the_spool_alone(self):
        self.runtime._lock_spool()
        self.addCleanup(self.runtime._spool_lock.close)
        other = Runtime(self.runtime.config, {"global": MagicMock()})
        with self.assertRaises(RuntimeError):
            other.run()
        # No clean-shutdown marker for the running pipeline's next recovery
        self.assertFalse(other.spool.path("clean-shutdown").exists())

    def test_window_not_blamed_for_another_crash(self):
        from src.gtfs_rt_aggregator.runtime.runtime import Task

        task = Task("window", "window /w", "normal", {"base": "/w"})
        for _ in range(5):
            self.runtime._finish(task, None, "worker died", crashed=True, alone=False)
            self.runtime._finish(task, None, "cancelled", cancelled=True)
        self.assertEqual(self.runtime._window_failures, {})
        self.runtime._finish(task, None, "boom")
        self.assertEqual(self.runtime._window_failures, {"/w": 1})


class TestWorkerWatch(unittest.TestCase):
    def test_worker_ends_with_the_main_process(self):
        import subprocess
        import sys

        # A pid that no longer runs
        gone = subprocess.Popen([sys.executable, "-c", "pass"])
        gone.wait()
        code = (
            "import time\n"
            "from src.gtfs_rt_aggregator.runtime.worker import _exit_with\n"
            f"_exit_with({gone.pid}, every_seconds=0.1)\n"
            "time.sleep(10)\n"
        )
        start = time.monotonic()
        result = subprocess.run([sys.executable, "-c", code], timeout=20)
        self.assertEqual(result.returncode, 1)
        self.assertLess(time.monotonic() - start, 8)


class TestDownload(unittest.TestCase):
    def test_trickling_download_stopped(self):
        import http.server
        import logging

        from src.gtfs_rt_aggregator.utils.http import DownloadTooLong, download_to

        class Trickle(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.send_header("Content-Length", "20")
                self.end_headers()
                try:
                    for _ in range(20):
                        self.wfile.write(b"x")
                        self.wfile.flush()
                        time.sleep(0.25)
                except OSError:
                    pass  # the client gave up, as expected

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Trickle)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        start = time.monotonic()
        with self.assertRaises(DownloadTooLong):
            download_to(
                f"http://127.0.0.1:{server.server_port}/",
                None,
                os.path.join(tmp, "f"),
                retries=0,
                logger=logging.getLogger(__name__),
                max_seconds=1,
            )
        # Stopped mid-body (sent in 5 s), not after it
        self.assertLess(time.monotonic() - start, 3)


if __name__ == "__main__":
    unittest.main()
