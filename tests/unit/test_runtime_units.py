"""Pieces of the runtime that are easier to test on their own."""

import os
import shutil
import tempfile
import threading
import time
import unittest
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

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

    def test_release_without_blame_counts_no_attempt(self):
        item = self.spool.claim(self._item())
        self.spool.release(item, "worker died", 3, count_attempt=False)
        (back,) = self.spool.queued_items()["f"]
        self.assertEqual(self.spool.meta(back)["attempt"], 1)
        self.spool.release(self.spool.claim(back), "boom", 3)
        (back,) = self.spool.queued_items()["f"]
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

    def test_fetch_waiting_for_static_keeps_its_attempts(self):
        from src.gtfs_rt_aggregator.runtime.runtime import Task

        feed = next(iter(self.runtime.feeds))
        item = self.runtime.spool.new_item(
            feed, datetime(2026, 9, 29, 8, tzinfo=timezone.utc)
        )
        tmp = item.with_name(item.name + ".part")
        tmp.write_bytes(b"x")
        self.runtime.spool.commit_item(item, tmp, {"feed": feed, "attempt": 1})
        claimed = self.runtime.spool.claim(item)
        task = Task("item", feed, "normal", {"item": str(claimed)})
        for _ in range(5):
            self.runtime._finish(task, None, "StaticNotReady: p: no static")
            (back,) = self.runtime._queues[feed]
            self.runtime._queues[feed].clear()
            task.info["item"] = str(self.runtime.spool.claim(back))
        meta = self.runtime.spool.meta(Path(task.info["item"]))
        self.assertEqual(meta["attempt"], 1)
        self.assertNotIn("suspect", meta)
        self.assertEqual(self.runtime.spool.quarantined(), 0)

    def test_window_not_blamed_for_another_crash(self):
        from src.gtfs_rt_aggregator.runtime.runtime import Task

        task = Task("window", "window /w", "normal", {"base": "/w"})
        for _ in range(5):
            self.runtime._finish(task, None, "worker died", blame=False)
            self.runtime._finish(task, None, "cancelled", cancelled=True)
        self.assertEqual(self.runtime._window_failures, {})
        self.runtime._finish(task, None, "boom")
        self.assertEqual(self.runtime._window_failures, {"/w": 1})


class TestFetchLane(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={"base_directory": tmp}),
            providers=[
                ProviderConfig(
                    name="p",
                    realtime=[ApiConfig(url="https://x.org/a", services=["TripUpdate"])],
                )
            ],
            runtime=RuntimeConfig(spool_dir=os.path.join(tmp, "spool")),
        )
        self.runtime = Runtime(config, {"global": MagicMock()})
        self.feed = next(iter(self.runtime.feeds))
        self.fast, self.slow = [], []
        self.runtime._fetch_pool = MagicMock(submit=lambda f, feed: self.fast.append(feed))
        self.runtime._slow_fetch_pool = MagicMock(
            submit=lambda f, feed: self.slow.append(feed)
        )

    def _download(self, body=b"feed"):
        def download(url, headers, path, *args, **kwargs):
            Path(path).write_bytes(body)
            import hashlib

            return len(body), hashlib.sha256(body).hexdigest()

        return patch("src.gtfs_rt_aggregator.runtime.runtime.download_to", download)

    def test_one_fetch_waits_behind_a_running_download(self):
        runtime, feed = self.runtime, self.feed
        runtime._submit_fetch(feed)
        # Still waiting for a thread: not queued twice
        runtime._submit_fetch(feed)
        self.assertEqual(self.fast, [feed])
        self.assertEqual(runtime._status[feed]["skipped_overlap"], 1)

        # Its download runs: the next turn starts as soon as it ends
        runtime._downloading.add(feed)
        runtime._submit_fetch(feed)
        runtime._submit_fetch(feed)
        self.assertEqual(self.fast, [feed])
        self.assertEqual(runtime._status[feed]["skipped_overlap"], 2)
        runtime._downloading.discard(feed)
        with self._download():
            runtime._fetch(feed)
        self.assertEqual(self.fast, [feed, feed])

    def test_slow_feed_gets_the_slow_threads(self):
        with (
            self._download(),
            patch("src.gtfs_rt_aggregator.runtime.runtime.SLOW_DOWNLOAD_SECONDS", -1),
        ):
            self.runtime._fetch(self.feed)
        self.runtime._submit_fetch(self.feed)
        self.assertEqual((self.fast, self.slow), ([], [self.feed]))

    def test_identical_downloads_recorded(self):
        runtime, feed = self.runtime, self.feed
        with self._download():
            for _ in range(3):
                runtime._fetch(feed)
        # The first one goes to a worker, the next two only leave their time
        self.assertEqual(len(runtime._queues[feed]), 1)
        self.assertEqual(len(runtime._unchanged[feed]), 2)

        # Ten minutes after the oldest: the next one goes to a worker anyway,
        # with their times
        runtime._unchanged[feed][0] = "2026-10-04T06:00:00+00:00"
        with self._download():
            runtime._fetch(feed)
        self.assertEqual(len(runtime._queues[feed]), 2)
        meta = runtime.spool.meta(runtime._queues[feed][1])
        self.assertEqual(len(meta["unchanged_fetch_times"]), 2)
        self.assertEqual(runtime._unchanged[feed], [])

    def test_undecodable_fetch_fetched_again_once(self):
        from src.gtfs_rt_aggregator.runtime.runtime import Task

        runtime, feed = self.runtime, self.feed
        with self._download(b"garbage"):
            runtime._fetch(feed)
        for expected in ([feed], [feed]):
            item = runtime._queues[feed].pop(0)
            claimed = runtime.spool.claim(item)
            runtime._finish(
                Task("item", feed, "normal", {"item": str(claimed)}),
                None,
                "DecodeError: Error parsing message",
            )
            self.assertEqual(self.fast, expected)
            # The fetch made again is marked: not fetched again if it fails too
            with self._download(b"garbage"):
                runtime._fetch(feed)
        self.assertEqual(runtime.spool.quarantined(), 2)

    def test_summary_line(self):
        runtime = self.runtime
        with self._download():
            runtime._fetch(self.feed)
        with self.assertLogs("src.gtfs_rt_aggregator.runtime.runtime", "INFO") as logs:
            runtime._log_summary()
        self.assertIn("1 fetches (0 changed), 0 failed", logs.output[0])
        self.assertEqual(runtime._summary, {})


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

    def _serve(self, handler):
        import http.server

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}/feed?key=SECRET"

    def test_cut_download_retried(self):
        import http.server
        import logging

        from src.gtfs_rt_aggregator.utils.http import download_to

        calls = []

        class Cut(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                calls.append(dict(self.headers))
                self.send_response(200)
                self.send_header("Content-Length", "20")
                self.end_headers()
                # The first answer stops halfway
                self.wfile.write(b"x" * (10 if len(calls) == 1 else 20))
                self.close_connection = True

            def log_message(self, *args):
                pass

        url = self._serve(Cut)
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        size, _ = download_to(
            url, None, os.path.join(tmp, "f"), retries=1,
            logger=logging.getLogger(__name__), max_seconds=10,
        )
        self.assertEqual((size, len(calls)), (20, 2))
        self.assertTrue(calls[0]["User-Agent"].startswith("gtfs-rt-aggregator"))

    def test_length_checked_unless_compressed(self):
        from src.gtfs_rt_aggregator.utils.http import IncompleteDownload, check_length

        response = MagicMock(url="https://x.org/f?key=SECRET")
        response.headers = {"Content-Length": "20"}
        with self.assertRaisesRegex(IncompleteDownload, "10 of 20") as raised:
            check_length(response, 10)
        self.assertNotIn("SECRET", str(raised.exception))
        check_length(response, 20)
        # The length of a gzip body is not the length received
        response.headers = {"Content-Length": "20", "Content-Encoding": "gzip"}
        check_length(response, 50)

    def test_cut_gzip_is_incomplete(self):
        import gzip

        from src.gtfs_rt_aggregator.utils.http import IncompleteDownload, check_gzip

        folder = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, folder)
        whole = gzip.compress(os.urandom(200_000))
        path = os.path.join(folder, "feed")
        with open(path, "wb") as f:
            f.write(whole)
        check_gzip(path)
        with open(path, "wb") as f:
            f.write(whole[: len(whole) // 2])
        with self.assertRaises(IncompleteDownload):
            check_gzip(path, "https://x.org/f.gz")
        # Not a gzip: not checked
        with open(path, "wb") as f:
            f.write(b"PK\x03\x04 a zip")
        check_gzip(path)

    def test_user_agent_and_error_body(self):
        import http.server
        import logging

        import requests

        from src.gtfs_rt_aggregator.utils.http import get_bytes, request_headers

        agents = []

        class Refuse(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                agents.append(self.headers.get("User-Agent"))
                body = (
                    b"<html>\n  Bad request: see https://api.example.org/doc?token=T0KEN"
                    + b" " + b"z" * 500
                )
                self.send_response(400)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        url = self._serve(Refuse)
        with self.assertRaises(requests.HTTPError) as raised:
            get_bytes(url, {"user-agent": "mine/1.0"}, 0, logging.getLogger(__name__))
        message = str(raised.exception)
        # What the server said, on one line, without keys from query strings
        self.assertIn("400", message)
        self.assertIn("Bad request: see https://api.example.org/doc?***", message)
        self.assertNotIn("SECRET", message)
        self.assertNotIn("T0KEN", message)
        self.assertLess(len(message), 400)
        # A User-Agent set in the configuration wins
        self.assertEqual(agents, ["mine/1.0"])
        self.assertEqual(request_headers({"X-Key": "k"})["X-Key"], "k")
        self.assertIn("User-Agent", request_headers(None))


if __name__ == "__main__":
    unittest.main()
