"""0.5.1: no API keys in manifests, SIGTERM, temp cleanup, peak memory logs."""

import json
import os
import signal
import socket
import tempfile
import time
import unittest

from src.gtfs_rt_aggregator.static.service import scrub_urls
from src.gtfs_rt_aggregator.utils.cleanup import clean_stale_temp_files
from src.gtfs_rt_aggregator.utils.scheduler import SchedulerClass, _run_job
from tests.mocks import MockStorageInterface
from tests.unit.test_static_service import TestStaticService, _FeedHandler


def _sleep(seconds):
    time.sleep(seconds)


class TestManifestUrls(TestStaticService):
    def _run(self, hour=None):
        # Same as the parent, with an API key in the query string
        self.url_with_key = self.url + "?key=SECRET"
        from datetime import datetime

        import pytz

        from tests.unit.test_static_service import _FakeDatetime

        if hour is not None:
            _FakeDatetime.current = datetime(2026, 9, 28, hour, 0, 0, tzinfo=pytz.UTC)
        self.service.run_once(
            provider_name="nl",
            feed_name="static",
            url=self.url_with_key,
            timezone="Europe/Amsterdam",
            headers={"x-api-key": "secret"},
            retries=0,
        )

    def test_no_key_saved_and_304_still_works(self):
        _FeedHandler.etag = '"v1"'
        self._run(1)
        self._run(2)
        for path in self.storage.list_paths("nl/static/"):
            self.assertNotIn(b"SECRET", self.storage.get_bytes(path), path)
        # The validators still apply to the same URL
        self.assertEqual(_FeedHandler.requests[1].get("If-None-Match"), '"v1"')

    def test_scrub_existing(self):
        storage = MockStorageInterface()
        manifest = {"version": "v", "url": "https://x.org/gtfs.zip?key=SECRET"}
        storage.save_bytes(json.dumps(manifest).encode(), "nl/static/v/manifest.json")
        storage.save_bytes(json.dumps(manifest).encode(), "nl/static/latest.json")
        storage.save_bytes(
            json.dumps({"version": "w", "url": "https://x.org/gtfs.zip"}).encode(),
            "nl/static/w/manifest.json",
        )
        self.assertEqual(scrub_urls(storage, "nl/static"), 2)
        for path in storage.list_paths("nl/static/"):
            self.assertNotIn(b"SECRET", storage.get_bytes(path))
        self.assertEqual(scrub_urls(storage, "nl/static"), 0)


class TestCleanup(unittest.TestCase):
    def test_stale_folders(self):
        with tempfile.TemporaryDirectory() as tmp:
            old = time.time() - 24 * 3600

            def folder(name, files=(), age=old):
                path = os.path.join(tmp, name)
                os.makedirs(path)
                for f in files:
                    open(os.path.join(path, f), "w").close()
                os.utime(path, (age, age))
                return path

            folder("gtfs_rt_aggregator-static-abc", ["feed.zip"])
            folder("tmpold", ["feed.zip"])  # 0.5.0 static work folder
            folder("tmpother", ["something.txt"])  # not ours
            folder("gtfs_rt_aggregator-static-new", ["feed.zip"], age=time.time())
            folder("pymp-dead", ["listener-x"])

            live = folder("pymp-live")
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(os.path.join(live, "listener-y"))
            server.listen(1)
            os.utime(live, (old, old))
            try:
                removed = clean_stale_temp_files(tmp)
            finally:
                server.close()

            self.assertEqual(removed, 3)
            self.assertEqual(
                sorted(os.listdir(tmp)),
                ["gtfs_rt_aggregator-static-new", "pymp-live", "tmpother"],
            )


class TestSignals(unittest.TestCase):
    def test_sigterm_stops_like_ctrl_c(self):
        sent = []

        def send_sigterm():
            if not sent:
                sent.append(True)
                os.kill(os.getpid(), signal.SIGTERM)
            return True

        before = signal.getsignal(signal.SIGTERM)
        scheduler = SchedulerClass(lifecycle_callback=[("sigterm", send_sigterm)])
        scheduler.add_schedules([(3600, _sleep, "sleeper", {"seconds": 30})])
        scheduler.start()  # returns instead of being killed

        self.assertFalse(scheduler.running)
        self.assertEqual(signal.getsignal(signal.SIGTERM), before)

    def test_job_processes_still_exit_on_sigterm(self):
        scheduler = SchedulerClass()
        previous = scheduler._handle_sigterm()
        try:
            scheduler._run_job_in_process(func=_sleep, job_label="sleeper", seconds=30)
            process = scheduler.processes[0]
            time.sleep(0.5)
            process.terminate()
            process.join(timeout=5)
            self.assertEqual(process.exitcode, -signal.SIGTERM)
        finally:
            signal.signal(signal.SIGTERM, previous)


class TestPeakMemory(unittest.TestCase):
    def test_logged(self):
        with self.assertLogs("src.gtfs_rt_aggregator.utils.scheduler", "INFO") as logs:
            _run_job(_sleep, "Fetcher - nl - VehiclePosition", {"seconds": 0})
        self.assertRegex(
            logs.output[0], r"Fetcher - nl - VehiclePosition in .*peak memory \d+ MB"
        )


if __name__ == "__main__":
    unittest.main()
