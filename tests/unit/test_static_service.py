import http.server
import io
import json
import threading
import unittest
import zipfile
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytz

from src.gtfs_rt_aggregator.config.models import (
    GtfsRtConfig,
    ProviderConfig,
    StaticConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.static.service import StaticService
from tests.mocks import MockStorageInterface

GTFS_FILES = {
    "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\n"
    "A1,Test Agency,https://example.com,Europe/Amsterdam\n",
    "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n"
    "S1,Stop One,52.37,4.89\n"
    "S2,Stop Two,52.38,4.90\n",
    "routes.txt": "route_id,agency_id,route_short_name,route_type\nR1,A1,1,3\n",
    "trips.txt": "route_id,service_id,trip_id\nR1,WD,T1\n",
    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
    "T1,08:00:00,08:00:00,S1,1\n"
    "T1,08:10:00,08:10:00,S2,2\n",
    "calendar.txt": "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
    "WD,1,1,1,1,1,0,0,20260101,20261231\n",
}


def _make_zip(files, date_time=(2026, 1, 1, 0, 0, 0)):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(zipfile.ZipInfo(name, date_time=date_time), content)
    return buffer.getvalue()


class _FeedHandler(http.server.BaseHTTPRequestHandler):
    body = b""
    etag = None
    status = 200
    requests = []

    def do_GET(self):
        type(self).requests.append(dict(self.headers))
        if self.status != 200:
            self.send_response(self.status)
            self.end_headers()
            return
        if self.etag and self.headers.get("If-None-Match") == self.etag:
            self.send_response(304)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Length", str(len(self.body)))
        if self.etag:
            self.send_header("ETag", self.etag)
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, format, *args):
        pass


class _FakeDatetime(datetime):
    current = datetime(2026, 9, 28, 1, 0, 0, tzinfo=pytz.UTC)

    @classmethod
    def now(cls, tz=None):
        return cls.current.astimezone(tz)


class TestStaticService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("localhost", 0), _FeedHandler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f"http://localhost:{cls.server.server_address[1]}/gtfs.zip"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def setUp(self):
        _FeedHandler.body = _make_zip(GTFS_FILES)
        _FeedHandler.etag = None
        _FeedHandler.status = 200
        _FeedHandler.requests = []
        _FakeDatetime.current = datetime(2026, 9, 28, 1, 0, 0, tzinfo=pytz.UTC)
        patcher = patch("src.gtfs_rt_aggregator.static.service.datetime", _FakeDatetime)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.storage = MockStorageInterface()
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={}),
            providers=[
                ProviderConfig(
                    name="nl",
                    timezone="Europe/Amsterdam",
                    static=[
                        StaticConfig(url=self.url, headers={"x-api-key": "secret"})
                    ],
                )
            ],
        )
        self.service = StaticService(config, {"global": self.storage})

    def _run(self, hour=None):
        if hour is not None:
            _FakeDatetime.current = datetime(2026, 9, 28, hour, 0, 0, tzinfo=pytz.UTC)
        self.service.run_once(
            provider_name="nl",
            feed_name="static",
            url=self.url,
            timezone="Europe/Amsterdam",
            headers={"x-api-key": "secret"},
            retries=0,
        )

    def _versions(self):
        return sorted(
            {
                path.split("/")[2]
                for path in self.storage.list_paths("nl/static/")
                if path.count("/") == 3
            }
        )

    def test_first_run_stores_full_version(self):
        self._run()

        self.assertEqual(self._versions(), ["2026-09-28_01-00-00Z"])
        version = "nl/static/2026-09-28_01-00-00Z"
        for table in ("agency", "stops", "routes", "trips", "stop_times", "calendar"):
            self.assertIn(f"{version}/{table}.parquet", self.storage.list_paths())

        stops = pd.read_parquet(
            io.BytesIO(self.storage.get_bytes(f"{version}/stops.parquet"))
        )
        self.assertEqual(len(stops), 2)

        manifest = json.loads(self.storage.get_bytes(f"{version}/manifest.json"))
        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        self.assertEqual(manifest, latest)
        self.assertEqual(set(manifest["files"]), set(GTFS_FILES))
        self.assertEqual(_FeedHandler.requests[0]["x-api-key"], "secret")

    def test_not_modified_with_etag(self):
        _FeedHandler.etag = '"v1"'
        self._run(1)
        self._run(2)

        self.assertEqual(_FeedHandler.requests[1].get("If-None-Match"), '"v1"')
        self.assertEqual(len(self._versions()), 1)

    def test_rebuilt_zip_with_same_files_not_stored(self):
        self._run(1)
        # Same content, new timestamps inside the zip, no ETag
        _FeedHandler.body = _make_zip(GTFS_FILES, date_time=(2026, 9, 28, 2, 0, 0))
        self._run(2)

        self.assertEqual(len(self._versions()), 1)

    def test_etag_refreshed_when_files_unchanged(self):
        _FeedHandler.etag = '"v1"'
        self._run(1)
        _FeedHandler.etag = '"v2"'
        self._run(2)

        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        self.assertEqual(latest["etag"], '"v2"')
        self.assertEqual(latest["version"], "2026-09-28_01-00-00Z")
        self.assertEqual(len(self._versions()), 1)

    def test_changed_file_stores_new_version(self):
        self._run(1)
        files = dict(GTFS_FILES)
        files["stops.txt"] += "S3,Stop Three,52.39,4.91\n"
        _FeedHandler.body = _make_zip(files)
        self._run(2)

        self.assertEqual(
            self._versions(), ["2026-09-28_01-00-00Z", "2026-09-28_02-00-00Z"]
        )
        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        self.assertEqual(latest["version"], "2026-09-28_02-00-00Z")
        stops = pd.read_parquet(
            io.BytesIO(
                self.storage.get_bytes(f"nl/static/{latest['version']}/stops.parquet")
            )
        )
        self.assertEqual(len(stops), 3)

    def test_not_a_gtfs_zip_stores_nothing(self):
        _FeedHandler.body = _make_zip({"readme.md": "nothing here"})
        self._run()
        self.assertEqual(self.storage.list_paths("nl/static/"), [])

    def test_files_in_subfolder_store_nothing(self):
        _FeedHandler.body = _make_zip({f"gtfs/{k}": v for k, v in GTFS_FILES.items()})
        self._run()
        self.assertEqual(self.storage.list_paths("nl/static/"), [])

    def test_server_error_keeps_latest(self):
        self._run(1)
        before = self.storage.get_bytes("nl/static/latest.json")
        _FeedHandler.status = 500
        self._run(2)

        self.assertEqual(self.storage.get_bytes("nl/static/latest.json"), before)
        self.assertEqual(len(self._versions()), 1)

    def test_unreadable_latest_is_ignored(self):
        self._run(1)
        self.storage.save_bytes(b'{"version": "trunc', "nl/static/latest.json")
        self._run(2)

        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        self.assertEqual(latest["version"], "2026-09-28_02-00-00Z")


class TestGtfsParquetVersion(unittest.TestCase):
    def test_version_tuple(self):
        from src.gtfs_rt_aggregator.static.service import _version_tuple

        self.assertEqual(_version_tuple("0.5.1"), (0, 5, 1))
        self.assertEqual(_version_tuple("0.5.2.dev3+g1234"), (0, 5, 2))
        self.assertEqual(_version_tuple("1.0rc1"), (1, 0))
        self.assertLess(_version_tuple("0.4.1"), (0, 5, 1))

    def test_old_gtfs_parquet_rejected(self):
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem"),
            providers=[
                ProviderConfig(
                    name="nl", static=[StaticConfig(url="https://example.org/a.zip")]
                )
            ],
        )
        with (
            patch(
                "src.gtfs_rt_aggregator.static.service.importlib.metadata.version",
                return_value="0.4.1",
            ),
            self.assertRaises(ImportError),
        ):
            StaticService(config, {"global": MockStorageInterface()})


if __name__ == "__main__":
    unittest.main()
