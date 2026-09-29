"""The disk-spool runtime, end to end, with a local HTTP server."""

import http.server
import json
import os
import shutil
import signal
import tempfile
import threading
import time
import unittest
from pathlib import Path

from google.transit import gtfs_realtime_pb2

from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    ProviderConfig,
    RuntimeConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.runtime.runtime import Runtime
from src.gtfs_rt_aggregator.storage.filesystem import FileSystemStorage

DATA = Path(__file__).parent.parent / "data"


class _Feed(http.server.BaseHTTPRequestHandler):
    """Serves the test vehicle feed, a little different at every request."""

    base = gtfs_realtime_pb2.FeedMessage()
    base.ParseFromString((DATA / "vehicle_positions.pb").read_bytes())
    del base.entity[200:]
    counter = 0
    lock = threading.Lock()

    def do_GET(self):
        if self.path.startswith("/gtfs.zip"):
            from tests.unit.test_static_service import GTFS_FILES, _make_zip

            body = _make_zip(GTFS_FILES)
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path.startswith("/garbage"):
            body = b"this is not a protobuf message" * 10
        else:
            with self.lock:
                type(self).counter += 1
                n = self.counter
            message = gtfs_realtime_pb2.FeedMessage()
            message.CopyFrom(self.base)
            message.header.timestamp += n
            message.entity[0].vehicle.position.latitude += n / 1000
            body = message.SerializeToString()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


class _FlakyStorage(FileSystemStorage):
    """Filesystem storage that fails while down is set."""

    def __init__(self, base):
        super().__init__(base)
        self.down = threading.Event()

    def save_file(self, local_path, path):
        if self.down.is_set():
            raise IOError("storage unavailable")
        return super().save_file(local_path, path)


class TestRuntime(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Feed)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = _FlakyStorage(os.path.join(self.tmp, "out"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _config(self, path="/vp.pb", **runtime):
        return GtfsRtConfig(
            storage=StorageConfig(
                type="filesystem",
                params={"base_directory": os.path.join(self.tmp, "out")},
            ),
            providers=[
                ProviderConfig(
                    name="p",
                    realtime=[
                        ApiConfig(
                            url=self.base_url + path,
                            services=["VehiclePosition"],
                            refresh_seconds=1,
                            retries=0,
                        )
                    ],
                )
            ],
            runtime=RuntimeConfig(
                spool_dir=os.path.join(self.tmp, "spool"),
                startup_jitter_seconds=0,
                workers=2,
                **runtime,
            ),
        )

    def _run(self, config, seconds, during=None):
        runtime = Runtime(config, {"global": self.storage})
        thread = threading.Thread(target=runtime.run)
        thread.start()
        try:
            if during:
                during(runtime)
            time.sleep(seconds)
        finally:
            runtime.stop()
            thread.join(timeout=120)
        self.assertFalse(thread.is_alive())
        return runtime

    def _individual(self):
        folder = Path(self.tmp, "out", "p", "VehiclePosition", "individual")
        return sorted(folder.glob("*.parquet")) if folder.exists() else []

    def _spool_items(self, *parts):
        return list(Path(self.tmp, "spool", *parts).glob("*/*.pb"))

    def test_end_to_end(self):
        self._run(self._config(), 6)
        files = self._individual()
        self.assertGreaterEqual(len(files), 3)
        status = json.loads(
            next(Path(self.tmp, "out", "p", "_status").glob("*.json")).read_text()
        )
        self.assertIn("last_success", status)
        self.assertEqual(status["kept_count"], 200)
        index = json.loads(Path(self.tmp, "out", "_status", "index.json").read_text())
        self.assertEqual(len(index["feeds"]), 1)
        # Everything processed and uploaded
        self.assertEqual(self._spool_items("incoming"), [])
        self.assertEqual(self._spool_items("processing"), [])
        self.assertEqual(list(Path(self.tmp, "spool", "ready").rglob("*.parquet")), [])

    def test_killed_worker_item_is_retried(self):
        def kill_workers(runtime):
            time.sleep(3)
            for pid in list(runtime._normal_pool._processes or {}):
                os.kill(pid, signal.SIGKILL)

        runtime = self._run(self._config(), 5, during=kill_workers)
        # The pool was replaced and every fetch processed: nothing waiting
        self.assertEqual(self._spool_items("incoming"), [])
        self.assertEqual(self._spool_items("processing"), [])
        self.assertEqual(self._spool_items("quarantine"), [])
        self.assertGreaterEqual(len(self._individual()), 5)
        # Each file once: names are fetch times, a retried fetch rewrites its own file
        names = [f.name for f in self._individual()]
        self.assertEqual(len(names), len(set(names)))

    def test_quarantine_after_max_attempts(self):
        from unittest.mock import patch

        # Retries wait 10 s, 20 s... in production
        with patch("src.gtfs_rt_aggregator.runtime.runtime.RETRY_BASE_SECONDS", 0.2):
            self._run(self._config("/garbage", max_attempts=2), 4)
        quarantined = self._spool_items("quarantine")
        self.assertGreaterEqual(len(quarantined), 1)
        meta = json.loads(quarantined[0].with_suffix(".json").read_text())
        self.assertEqual(meta["attempt"], 3)
        self.assertIn("last_error", meta)
        self.assertEqual(self._individual(), [])

    def test_storage_outage(self):
        self.storage.down.set()

        def restore(runtime):
            time.sleep(4)
            ready = list(Path(self.tmp, "spool", "ready").rglob("*.parquet"))
            self.assertGreaterEqual(len(ready), 2)  # waiting on disk
            self.assertEqual(self._individual(), [])
            self.storage.down.clear()

        self._run(self._config(), 6, during=restore)
        self.assertGreaterEqual(len(self._individual()), 4)
        self.assertEqual(list(Path(self.tmp, "spool", "ready").rglob("*.parquet")), [])

    def test_static_feed(self):
        from src.gtfs_rt_aggregator.config.models import StaticConfig

        config = self._config()
        config.providers[0].static.append(StaticConfig(url=self.base_url + "/gtfs.zip"))
        self._run(config, 8)
        latest = json.loads(
            Path(self.tmp, "out", "p", "static", "latest.json").read_text()
        )
        for path in latest["tables"].values():
            self.assertTrue(Path(self.tmp, "out", path).exists(), path)
        # The rows carry the static version once it is stored
        import pyarrow.parquet as pq

        versions = set()
        for file in self._individual():
            versions |= set(
                pq.read_table(file, columns=["staticVersion"])[
                    "staticVersion"
                ].to_pylist()
            )
        self.assertIn(latest["version"], versions)
        self.assertEqual(list(Path(self.tmp, "spool", "static").rglob("feed.zip")), [])

    def test_spool_full_pauses_fetching(self):
        from unittest.mock import patch

        # The size is checked every 10 s in production
        with patch(
            "src.gtfs_rt_aggregator.runtime.runtime.SPOOL_SIZE_EVERY_SECONDS", 1
        ):
            runtime = self._run(self._config(spool_max_gb=1e-9), 4)
        status = runtime._status[next(iter(runtime.feeds))]
        self.assertGreaterEqual(status.get("skipped_spool_full", 0), 1)
        index = json.loads(Path(self.tmp, "out", "_status", "index.json").read_text())
        self.assertTrue(next(iter(index["feeds"].values()))["paused"])


if __name__ == "__main__":
    unittest.main()
