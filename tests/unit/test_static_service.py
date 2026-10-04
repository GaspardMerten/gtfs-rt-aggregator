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
    last_modified = None
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
        if self.last_modified:
            self.send_header("Last-Modified", self.last_modified)
        self.end_headers()
        self.wfile.write(self.body)

    def log_message(self, format, *args):
        pass


class _FakeDatetime(datetime):
    current = datetime(2026, 9, 28, 1, 0, 0, tzinfo=pytz.UTC)

    @classmethod
    def now(cls, tz=None):
        return cls.current.astimezone(tz)


class _StaticTestCase(unittest.TestCase):
    """A local server serving the feed, and the service storing it."""

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
        _FeedHandler.last_modified = None
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

    def _run(self, hour=None, reuse_unchanged_tables=False):
        if hour is not None:
            _FakeDatetime.current = datetime(2026, 9, 28, hour, 0, 0, tzinfo=pytz.UTC)
        self.service.run_once(
            provider_name="nl",
            feed_name="static",
            url=self.url,
            timezone="Europe/Amsterdam",
            headers={"x-api-key": "secret"},
            retries=0,
            reuse_unchanged_tables=reuse_unchanged_tables,
        )

    def _versions(self):
        return sorted(
            {
                path.split("/")[2]
                for path in self.storage.list_paths("nl/static/")
                if path.count("/") == 3
            }
        )


class TestStaticService(_StaticTestCase):
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

    def test_older_file_than_the_stored_one_not_stored(self):
        # A publisher with a stale second server: versions must not flip back to its older file
        _FeedHandler.last_modified = "Wed, 01 Oct 2026 10:00:00 GMT"
        self._run(1)
        changed = dict(GTFS_FILES, **{"stops.txt": GTFS_FILES["stops.txt"] + "S9,Extra,52.0,4.0\n"})
        _FeedHandler.body = _make_zip(changed)
        _FeedHandler.last_modified = "Wed, 24 Sep 2026 10:00:00 GMT"
        self._run(2)

        self.assertEqual(len(self._versions()), 1)

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


def _mixed_feed(trains=20, retimed=0, buses=5):
    """Feed with `trains` rail trips (the first `retimed` 1 min later) and `buses` bus trips."""
    trips, times = ["route_id,service_id,trip_id,shape_id"], [
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence"
    ]
    for i in range(trains):
        trips.append(f"RAIL,WD,T{i},SH1")
        minute = 1 if i < retimed else 0
        times += [f"T{i},08:0{minute}:00,08:0{minute}:00,S1,1", f"T{i},08:10:00,08:10:00,S2,2"]
    for i in range(buses):
        trips.append(f"BUS,WE,B{i},SH2")
        times += [f"B{i},09:00:00,09:00:00,S2,1", f"B{i},09:10:00,09:10:00,S3,2"]
    files = dict(GTFS_FILES)
    files.update(
        {
            "stops.txt": GTFS_FILES["stops.txt"] + "S3,Stop Three,52.39,4.91\n",
            "routes.txt": "route_id,agency_id,route_short_name,route_type\n"
            "RAIL,A1,IC,2\nBUS,A1,10,3\n",
            "trips.txt": "\n".join(trips) + "\n",
            "stop_times.txt": "\n".join(times) + "\n",
            "calendar.txt": GTFS_FILES["calendar.txt"]
            + "WE,0,0,0,0,0,1,1,20260101,20261231\n",
            "shapes.txt": "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
            "SH1,52.37,4.89,1\nSH2,52.38,4.90,1\n",
        }
    )
    return files


class TestStaticChangeRules(_StaticTestCase):
    """min_change, max_days and route_types."""

    def setUp(self):
        super().setUp()
        _FeedHandler.body = _make_zip(_mixed_feed())

    def _configure(self, **static):
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={}),
            providers=[
                ProviderConfig(
                    name="nl",
                    timezone="Europe/Amsterdam",
                    static=[StaticConfig(url=self.url, **static)],
                )
            ],
        )
        self.service = StaticService(config, {"global": self.storage})

    def _table(self, name):
        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        return pd.read_parquet(io.BytesIO(self.storage.get_bytes(latest["tables"][name])))

    def test_small_change_not_stored(self):
        self._configure(min_change=0.1)
        self._run(1)
        # 2 of 25 trips retimed: 8%
        _FeedHandler.body = _make_zip(_mixed_feed(retimed=2))
        self._run(2)

        self.assertEqual(self._versions(), ["2026-09-28_01-00-00Z"])
        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        self.assertEqual(latest["checked_change"], 0.08)
        self.assertIn("checked_files", latest)
        # The same feed again is recognised without converting it
        with patch.object(StaticService, "_convert") as convert:
            self._run(3)
        convert.assert_not_called()

    def test_large_change_stored(self):
        self._configure(min_change=0.1)
        self._run(1)
        _FeedHandler.body = _make_zip(_mixed_feed(retimed=3))
        self._run(2)

        self.assertEqual(len(self._versions()), 2)

    def test_added_and_removed_trips_count(self):
        self._configure(min_change=0.1)
        self._run(1)
        # 3 trips gone out of 25
        _FeedHandler.body = _make_zip(_mixed_feed(trains=17))
        self._run(2)

        self.assertEqual(len(self._versions()), 2)

    def test_renumbered_services_not_a_change(self):
        self._configure(min_change=0.1)
        self._run(1)
        files = _mixed_feed()
        for name in ("trips.txt", "calendar.txt"):
            files[name] = files[name].replace("WD", "000001").replace("WE", "000002")
        _FeedHandler.body = _make_zip(files)
        self._run(2)

        self.assertEqual(len(self._versions()), 1)

    def test_changed_days_count(self):
        self._configure(min_change=0.1)
        self._run(1)
        # The weekday trains no longer run on Wednesday 30 September
        files = _mixed_feed()
        files["calendar_dates.txt"] = "service_id,date,exception_type\nWD,20260930,2\n"
        _FeedHandler.body = _make_zip(files)
        self._run(2)

        self.assertEqual(len(self._versions()), 2)

    def test_trips_after_the_coming_days_ignored(self):
        self._configure(min_change=0.1)
        self._run(1)
        # A new timetable from December, the trains of the coming days unchanged
        files = _mixed_feed()
        files["trips.txt"] += "".join(f"RAIL,DEC,X{i},SH1\n" for i in range(20))
        files["stop_times.txt"] += "".join(
            f"X{i},07:00:00,07:00:00,S1,1\nX{i},07:10:00,07:10:00,S2,2\n" for i in range(20)
        )
        files["calendar.txt"] += "DEC,1,1,1,1,1,1,1,20261213,20271211\n"
        _FeedHandler.body = _make_zip(files)
        self._run(2)

        self.assertEqual(len(self._versions()), 1)

    def test_max_days_stores_small_change(self):
        self._configure(min_change=0.5, max_days=7)
        self._run(1)
        _FeedHandler.body = _make_zip(_mixed_feed(retimed=1))
        self._run(2)
        self.assertEqual(len(self._versions()), 1)
        _FakeDatetime.current = datetime(2026, 10, 5, 1, 0, 0, tzinfo=pytz.UTC)
        self._run()

        self.assertEqual(len(self._versions()), 2)

    def test_version_without_signatures_gets_them(self):
        # Stored without min_change: no signatures
        self._run(1)
        self.assertNotIn("signatures", json.loads(self.storage.get_bytes("nl/static/latest.json")))
        self._configure(min_change=0.1)
        _FeedHandler.body = _make_zip(_mixed_feed(retimed=1))
        self._run(2)

        self.assertEqual(len(self._versions()), 1)
        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        self.assertEqual(
            latest["signatures"], "nl/static/2026-09-28_01-00-00Z/_trip_signatures.parquet"
        )

    def test_route_types_keep_rail_only(self):
        self._configure(route_types=[2, "100-199"])
        self._run(1)

        self.assertEqual(list(self._table("routes")["route_id"]), ["RAIL"])
        self.assertEqual(len(self._table("trips")), 20)
        self.assertEqual(set(self._table("stop_times")["trip_id"]), {f"T{i}" for i in range(20)})
        self.assertEqual(list(self._table("calendar")["service_id"]), ["WD"])
        self.assertEqual(list(self._table("shapes")["shape_id"]), ["SH1"])
        # Stops are all kept
        self.assertEqual(len(self._table("stops")), 3)
        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        self.assertEqual(latest["route_types"][:2], [2, 100])

    def test_route_types_ignore_other_modes_changes(self):
        self._configure(route_types=[2], min_change=0.01)
        self._run(1)
        _FeedHandler.body = _make_zip(_mixed_feed(buses=50))
        self._run(2)

        self.assertEqual(len(self._versions()), 1)

    def test_new_route_types_store_a_version(self):
        self._configure(min_change=0.5)
        self._run(1)
        self._configure(route_types=[2], min_change=0.5)
        _FeedHandler.body = _make_zip(_mixed_feed(retimed=1))
        self._run(2)

        self.assertEqual(len(self._versions()), 2)
        self.assertEqual(len(self._table("trips")), 20)

    def test_filtered_tables_not_reused_when_trips_change(self):
        self._configure(route_types=[2])
        self._run(1, reuse_unchanged_tables=True)
        # Only trips.txt changes: calendar.txt is unchanged, but WD is no longer used
        files = _mixed_feed()
        files["trips.txt"] = files["trips.txt"].replace(",WD,", ",WE,")
        _FeedHandler.body = _make_zip(files)
        self._run(2, reuse_unchanged_tables=True)

        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        self.assertTrue(latest["tables"]["calendar"].startswith(f"nl/static/{latest['version']}/"))
        self.assertEqual(list(self._table("calendar")["service_id"]), ["WE"])
        # Unfiltered and unchanged: reused
        self.assertTrue(latest["tables"]["stops"].startswith("nl/static/2026-09-28_01-00-00Z/"))

    def test_new_route_types_on_unchanged_feed_stored(self):
        self._run(1)
        self._configure(route_types=[2])
        self._run(2)

        self.assertEqual(len(self._versions()), 2)
        self.assertEqual(len(self._table("trips")), 20)

    def test_new_route_types_read_the_file_despite_its_etag(self):
        # The server would answer 304: the new filter must still be applied to the unchanged file
        _FeedHandler.etag = '"same"'
        self._run(1)
        self._configure(route_types=[2])
        self._run(2)

        self.assertEqual(len(self._versions()), 2)
        self.assertNotIn("If-None-Match", _FeedHandler.requests[-1])

    def test_no_route_of_route_types_stores_nothing(self):
        self._configure(route_types=[1])
        self._run(1)
        self.assertEqual(self._versions(), [])


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


NETEX = """<?xml version="1.0" encoding="UTF-8"?>
<PublicationDelivery xmlns="http://www.netex.org.uk/netex">
 <dataObjects><CompositeFrame id="C">
  <FrameDefaults><DefaultLocale><TimeZone>Europe/Rome</TimeZone></DefaultLocale></FrameDefaults>
  <frames>
   <ResourceFrame id="R"><organisations><Operator id="OP"><Name>Rail Co</Name></Operator></organisations></ResourceFrame>
   <ServiceFrame id="SF">
    <lines>
     <Line id="L:REG"><Name>Regionale</Name><TransportMode>rail</TransportMode></Line>
     <Line id="L:BUS"><Name>Autobus</Name><TransportMode>bus</TransportMode></Line>
    </lines>
    <scheduledStopPoints>
     <ScheduledStopPoint id="A"><Name>Alpha</Name><Location><Longitude>9.1</Longitude><Latitude>45.4</Latitude></Location></ScheduledStopPoint>
     <ScheduledStopPoint id="B"><Name>Beta</Name><Location><Longitude>9.2</Longitude><Latitude>45.5</Latitude></Location></ScheduledStopPoint>
    </scheduledStopPoints>
    <journeyPatterns>
     <ServiceJourneyPattern id="JP:REG"><RouteView><LineRef ref="L:REG"/></RouteView><pointsInSequence>
      <StopPointInJourneyPattern id="P1" order="1"><ScheduledStopPointRef ref="A"/></StopPointInJourneyPattern>
      <StopPointInJourneyPattern id="P2" order="2"><ScheduledStopPointRef ref="B"/></StopPointInJourneyPattern>
     </pointsInSequence></ServiceJourneyPattern>
     <ServiceJourneyPattern id="JP:BUS"><RouteView><LineRef ref="L:BUS"/></RouteView><pointsInSequence>
      <StopPointInJourneyPattern id="P3" order="1"><ScheduledStopPointRef ref="B"/></StopPointInJourneyPattern>
      <StopPointInJourneyPattern id="P4" order="2"><ScheduledStopPointRef ref="A"/></StopPointInJourneyPattern>
     </pointsInSequence></ServiceJourneyPattern>
    </journeyPatterns>
   </ServiceFrame>
   <TimetableFrame id="TF"><vehicleJourneys>
    <ServiceJourney id="SJ:1"><Name>10201</Name><dayTypes><DayTypeRef ref="DT"/></dayTypes><ServiceJourneyPatternRef ref="JP:REG"/>
     <passingTimes>
      <TimetabledPassingTime><StopPointInJourneyPatternRef ref="P1"/><DepartureTime>08:00:00</DepartureTime></TimetabledPassingTime>
      <TimetabledPassingTime><StopPointInJourneyPatternRef ref="P2"/><ArrivalTime>08:30:00</ArrivalTime></TimetabledPassingTime>
     </passingTimes></ServiceJourney>
    <ServiceJourney id="SJ:2"><Name>B1</Name><dayTypes><DayTypeRef ref="DT"/></dayTypes><ServiceJourneyPatternRef ref="JP:BUS"/>
     <passingTimes>
      <TimetabledPassingTime><StopPointInJourneyPatternRef ref="P3"/><DepartureTime>09:00:00</DepartureTime></TimetabledPassingTime>
      <TimetabledPassingTime><StopPointInJourneyPatternRef ref="P4"/><ArrivalTime>09:30:00</ArrivalTime></TimetabledPassingTime>
     </passingTimes></ServiceJourney>
   </vehicleJourneys></TimetableFrame>
   <ServiceCalendarFrame id="CF">
    <operatingPeriods><UicOperatingPeriod id="OP:1"><FromDate>2026-09-28T00:00:00</FromDate><ToDate>2026-10-04T00:00:00</ToDate><ValidDayBits>1111100</ValidDayBits></UicOperatingPeriod></operatingPeriods>
    <dayTypeAssignments><DayTypeAssignment id="DTA" order="1"><OperatingPeriodRef ref="OP:1"/><DayTypeRef ref="DT"/></DayTypeAssignment></dayTypeAssignments>
   </ServiceCalendarFrame>
  </frames>
 </CompositeFrame></dataObjects>
</PublicationDelivery>
"""


def _gzip(text, mtime=0):
    import gzip

    return gzip.compress(text.encode(), mtime=mtime)


class TestNetexFeed(_StaticTestCase):
    def setUp(self):
        super().setUp()
        _FeedHandler.body = _gzip(NETEX)

    def _configure(self, **static):
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={}),
            providers=[
                ProviderConfig(
                    name="nl",
                    timezone="Europe/Amsterdam",
                    static=[StaticConfig(url=self.url, format="netex", **static)],
                )
            ],
        )
        self.service = StaticService(config, {"global": self.storage})

    def _table(self, name):
        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        return pd.read_parquet(io.BytesIO(self.storage.get_bytes(latest["tables"][name])))

    def test_stored_as_gtfs_tables(self):
        self._configure(route_types=[2])
        self._run(1)

        self.assertEqual(self._versions(), ["2026-09-28_01-00-00Z"])
        self.assertEqual(list(self._table("trips")["trip_short_name"]), ["10201"])
        self.assertEqual(len(self._table("calendar_dates")), 5)
        latest = json.loads(self.storage.get_bytes("nl/static/latest.json"))
        self.assertEqual(list(latest["files"]), ["netex.xml"])

    def test_recompressed_file_not_stored_again(self):
        self._configure()
        self._run(1)
        # Same XML, gzipped at another time: the gzip header differs
        _FeedHandler.body = _gzip(NETEX, mtime=1_000_000)
        self._run(2)
        self.assertEqual(len(self._versions()), 1)

        _FeedHandler.body = _gzip(NETEX.replace("08:30:00", "08:35:00"))
        self._run(3)
        self.assertEqual(len(self._versions()), 2)

    def test_adapter_and_netex_rejected(self):
        with self.assertRaises(ValueError):
            StaticConfig(adapter="module:function", format="netex")
