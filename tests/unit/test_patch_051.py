"""0.5.1: no API keys in manifests, temp cleanup."""

import json
import os
import socket
import tempfile
import time
import unittest

from src.gtfs_rt_aggregator.static.service import scrub_urls
from src.gtfs_rt_aggregator.utils.cleanup import clean_stale_temp_files
from tests.mocks import MockStorageInterface
from tests.unit.test_static_service import TestStaticService, _FeedHandler


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


if __name__ == "__main__":
    unittest.main()
