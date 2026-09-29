"""trip_stop_events from synthetic SNCB-like (delays, all stops) and Entur-like (times, upcoming stops) feeds."""

import os
import tempfile
import unittest
from datetime import date, datetime, timedelta

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytz
from google.transit import gtfs_realtime_pb2

from src.gtfs_rt_aggregator.aggregator.trip_stop_events import (
    StaticTimetable,
    build_trip_stop_events,
    service_day_base,
)
from src.gtfs_rt_aggregator.fetcher.gtfs_rt import GtfsRtFetcher

TZ = pytz.timezone("Europe/Brussels")
D = date(2026, 10, 24)  # Saturday; the night after is the end of DST


def _duration(text):
    h, m, s = (int(x) for x in text.split(":"))
    return timedelta(hours=h, minutes=m, seconds=s)


def _static(folder, stop_times, trips, frequencies=()):
    os.makedirs(folder, exist_ok=True)
    if frequencies:
        pl.DataFrame({"trip_id": list(frequencies)}).write_parquet(
            os.path.join(folder, "frequencies.parquet")
        )
    pl.DataFrame(
        [
            {
                "trip_id": t,
                "stop_sequence": seq,
                "stop_id": stop,
                "arrival_time": _duration(arr),
                "departure_time": _duration(dep),
            }
            for t, stops in stop_times.items()
            for seq, stop, arr, dep in stops
        ],
        schema={
            "trip_id": pl.Utf8,
            "stop_sequence": pl.Int16,
            "stop_id": pl.Utf8,
            "arrival_time": pl.Duration("ms"),
            "departure_time": pl.Duration("ms"),
        },
    ).write_parquet(os.path.join(folder, "stop_times.parquet"))
    pl.DataFrame(
        [{"trip_id": t, "route_id": r, "service_id": "S"} for t, r in trips.items()]
    ).write_parquet(os.path.join(folder, "trips.parquet"))
    pl.DataFrame(
        [
            {
                "service_id": "S",
                **{
                    d: 1
                    for d in (
                        "monday",
                        "tuesday",
                        "wednesday",
                        "thursday",
                        "friday",
                        "saturday",
                        "sunday",
                    )
                },
                "start_date": date(2026, 1, 1),
                "end_date": date(2026, 12, 31),
            }
        ]
    ).with_columns(
        pl.col(
            [
                "monday",
                "tuesday",
                "wednesday",
                "thursday",
                "friday",
                "saturday",
                "sunday",
            ]
        ).cast(pl.Int8)
    ).write_parquet(
        os.path.join(folder, "calendar.parquet")
    )
    return StaticTimetable(folder)


def _local(day, hhmm):
    h, m = (int(x) for x in hhmm.split(":"))
    return TZ.localize(datetime(day.year, day.month, day.day, h, m))


class _Feed:
    """TripUpdate Parquet files, one per fetch."""

    def __init__(self, folder):
        self.folder = folder
        self.files = []

    def fetch(self, at: datetime, trips, version="v1", seen=None):
        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        for index, trip in enumerate(trips):
            entity = message.entity.add(id=f"e{index}")
            update = entity.trip_update
            for key in ("trip_id", "route_id", "start_time", "start_date"):
                if key in trip:
                    setattr(update.trip, key, trip[key])
            if "relationship" in trip:
                update.trip.schedule_relationship = getattr(
                    gtfs_realtime_pb2.TripDescriptor, trip["relationship"]
                )
            for stop in trip.get("stops", []):
                stu = update.stop_time_update.add()
                if "seq" in stop:
                    stu.stop_sequence = stop["seq"]
                if "stop" in stop:
                    stu.stop_id = stop["stop"]
                for kind in ("arrival", "departure"):
                    if f"{kind}_delay" in stop:
                        getattr(stu, kind).delay = stop[f"{kind}_delay"]
                    if f"{kind}_time" in stop:
                        getattr(stu, kind).time = int(stop[f"{kind}_time"].timestamp())
                if stop.get("skipped"):
                    stu.schedule_relationship = (
                        gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SKIPPED
                    )
                if stop.get("no_data"):
                    stu.schedule_relationship = (
                        gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.NO_DATA
                    )
        entities = list(message.entity)
        table = GtfsRtFetcher.build_tables(
            entities,
            [GtfsRtFetcher.entity_hash(e) for e in entities],
            ["TripUpdate"],
            at,
            {"staticVersion": version},
        )["TripUpdate"]
        if seen is not None:
            # Rows merged by deduplication: seen from first to last
            for name, value in zip(("firstSeen", "lastSeen"), seen):
                table = table.append_column(
                    pa.field(name, pa.timestamp("us", "UTC")),
                    pa.array([value] * table.num_rows, pa.timestamp("us", "UTC")),
                )
        path = os.path.join(self.folder, f"{len(self.files)}.parquet")
        pq.write_table(table, path)
        self.files.append(path)


class TestTripStopEvents(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp = tmp.name
        self.timetable = _static(
            os.path.join(self.tmp, "v1"),
            {
                "T1": [
                    (1, "A", "08:00:00", "08:00:00"),
                    (2, "B", "08:10:00", "08:11:00"),
                    (3, "C", "08:20:00", "08:20:00"),
                ],
                "T2": [
                    (1, "A", "09:00:00", "09:00:00"),
                    (2, "C", "09:20:00", "09:20:00"),
                ],
                "T3": [
                    (1, "A", "23:50:00", "23:50:00"),
                    (2, "C", "24:20:00", "24:20:00"),
                ],
                "T4": [
                    (1, "A", "10:00:00", "10:00:00"),
                    (2, "B", "10:10:00", "10:10:00"),
                    (3, "C", "10:20:00", "10:20:00"),
                ],
                "T5": [
                    (1, "A", "01:30:00", "01:30:00"),
                    (2, "C", "03:30:00", "03:30:00"),
                ],
                # Frequency-based: template run at 06:00
                "F1": [
                    (1, "A", "06:00:00", "06:00:00"),
                    (2, "B", "06:10:00", "06:10:00"),
                ],
            },
            {"T1": "R1", "T2": "R1", "T3": "R2", "T4": "R1", "T5": "R3", "F1": "R4"},
            frequencies=["F1"],
        )
        self.feed = _Feed(self.tmp)

    def _build(self, day=D):
        frame = build_trip_stop_events(
            self.feed.files, day, "Europe/Brussels", "be", {"v1": self.timetable}
        )
        return frame

    @staticmethod
    def _row(frame, trip, sequence):
        rows = frame.filter(
            (pl.col("trip_id") == trip) & (pl.col("stop_sequence") == sequence)
        ).to_dicts()
        assert len(rows) == 1, rows
        return rows[0]

    # SNCB-like: delays only, every stop, startDate given
    def test_delays_first_last_observed(self):
        day = D.strftime("%Y%m%d")
        self.feed.fetch(
            _local(D, "07:50"),
            [
                {
                    "trip_id": "T1",
                    "start_date": day,
                    "stops": [
                        {
                            "seq": s,
                            "stop": st,
                            "arrival_delay": 60,
                            "departure_delay": 60,
                        }
                        for s, st in ((1, "A"), (2, "B"), (3, "C"))
                    ],
                }
            ],
        )
        self.feed.fetch(
            _local(D, "08:15"),
            [
                {
                    "trip_id": "T1",
                    "start_date": day,
                    "stops": [
                        {
                            "seq": 1,
                            "stop": "A",
                            "arrival_delay": 60,
                            "departure_delay": 60,
                        },
                        {
                            "seq": 2,
                            "stop": "B",
                            "arrival_delay": 120,
                            "departure_delay": 120,
                        },
                        {"seq": 3, "stop": "C", "arrival_delay": 120},
                    ],
                }
            ],
        )
        # Another day's run of the same trip is ignored
        self.feed.fetch(
            _local(D + timedelta(days=1), "07:50"),
            [
                {
                    "trip_id": "T1",
                    "start_date": (D + timedelta(days=1)).strftime("%Y%m%d"),
                    "stops": [{"seq": 1, "stop": "A", "arrival_delay": 999}],
                }
            ],
        )
        frame = self._build()
        self.assertEqual(frame.height, 3)
        a, b, c = (self._row(frame, "T1", s) for s in (1, 2, 3))
        self.assertEqual(a["scheduled_arrival"], int(_local(D, "08:00").timestamp()))
        self.assertEqual((a["first_arrival_delay"], a["last_arrival_delay"]), (60, 60))
        self.assertEqual((b["first_arrival_delay"], b["last_arrival_delay"]), (60, 120))
        self.assertEqual(
            b["last_predicted_departure"], int(_local(D, "08:13").timestamp())
        )
        self.assertEqual(c["prediction_count"], 2)
        self.assertEqual(a["route_id"], "R1")  # from the timetable
        self.assertTrue(a["observed"])
        self.assertTrue(b["observed"])  # 08:15 >= 08:13
        self.assertFalse(c["observed"])
        self.assertFalse(c["delay_propagated"])
        self.assertEqual(a["date"], D)

    def test_canceled_trip_has_its_static_stops(self):
        self.feed.fetch(
            _local(D, "08:30"),
            [
                {
                    "trip_id": "T2",
                    "start_date": D.strftime("%Y%m%d"),
                    "relationship": "CANCELED",
                }
            ],
        )
        frame = self._build()
        self.assertEqual(frame["stop_sequence"].to_list(), [1, 2])
        row = self._row(frame, "T2", 2)
        self.assertEqual(row["trip_schedule_relationship"], "CANCELED")
        self.assertEqual(row["prediction_count"], 0)
        self.assertIsNone(row["last_arrival_delay"])
        self.assertFalse(row["observed"])

    def test_skipped_stop_and_propagation(self):
        self.feed.fetch(
            _local(D, "09:55"),
            [
                {
                    "trip_id": "T4",
                    "start_date": D.strftime("%Y%m%d"),
                    "stops": [
                        {"seq": 1, "departure_delay": 300},
                        {"seq": 2, "skipped": True},
                    ],
                }
            ],
        )
        frame = self._build()
        a, b, c = (self._row(frame, "T4", s) for s in (1, 2, 3))
        self.assertEqual(a["last_departure_delay"], 300)
        self.assertEqual(b["stop_schedule_relationship"], "SKIPPED")
        self.assertIsNone(b["last_arrival_delay"])
        self.assertIsNone(b["last_predicted_arrival"])
        self.assertFalse(b["delay_propagated"])
        self.assertTrue(c["delay_propagated"])
        self.assertEqual(c["last_arrival_delay"], 300)
        self.assertEqual(
            c["last_predicted_arrival"], int(_local(D, "10:25").timestamp())
        )
        self.assertEqual(c["prediction_count"], 0)

    def test_trip_past_midnight(self):
        after = D + timedelta(days=1)
        self.feed.fetch(
            _local(after, "00:10"),
            [
                {
                    "trip_id": "T3",
                    "start_date": D.strftime("%Y%m%d"),
                    "stops": [{"seq": 2, "arrival_delay": 30}],
                }
            ],
        )
        frame = self._build()
        c = self._row(frame, "T3", 2)
        self.assertEqual(
            c["scheduled_arrival"], int(_local(after, "00:20").timestamp())
        )
        self.assertEqual(
            c["last_predicted_arrival"], int(_local(after, "00:20").timestamp()) + 30
        )
        # Stops before the first update get nothing
        self.assertIsNone(self._row(frame, "T3", 1)["last_arrival_delay"])

    def test_dst_night(self):
        dst = D + timedelta(days=1)  # clocks go back at 03:00 CEST
        base = service_day_base(dst, TZ)
        self.assertEqual(base, pytz.utc.localize(datetime(2026, 10, 24, 23)))
        # 01:30 in GTFS time is 02:30 CEST (00:30 UTC), 03:30 is 02:30 CET
        self.feed.fetch(
            pytz.utc.localize(datetime(2026, 10, 25, 0, 0)),
            [
                {
                    "trip_id": "T5",
                    "start_date": dst.strftime("%Y%m%d"),
                    "stops": [
                        {"seq": 1, "departure_delay": 0},
                        {"seq": 2, "arrival_delay": 60},
                    ],
                }
            ],
        )
        frame = self._build(dst)
        a, c = self._row(frame, "T5", 1), self._row(frame, "T5", 2)
        self.assertEqual(
            a["scheduled_departure"],
            int(pytz.utc.localize(datetime(2026, 10, 25, 0, 30)).timestamp()),
        )
        self.assertEqual(
            c["scheduled_arrival"],
            int(pytz.utc.localize(datetime(2026, 10, 25, 2, 30)).timestamp()),
        )

    # Entur-like: times only, upcoming stops only, stop ids, no startDate
    def test_times_upcoming_stops_without_start_date(self):
        def times(day, stops):
            return [
                {
                    "stop": s,
                    "arrival_time": _local(day, t),
                    "departure_time": _local(day, t),
                }
                for s, t in stops
            ]

        before = D - timedelta(days=1)
        # The previous day's run: same trip id, not part of D
        self.feed.fetch(
            _local(before, "07:55"),
            [
                {
                    "trip_id": "T1",
                    "stops": times(
                        before, [("A", "08:30"), ("B", "08:40"), ("C", "08:50")]
                    ),
                }
            ],
        )
        self.feed.fetch(
            _local(D, "07:55"),
            [
                {
                    "trip_id": "T1",
                    "stops": times(D, [("A", "08:02"), ("B", "08:12"), ("C", "08:22")]),
                }
            ],
        )
        self.feed.fetch(
            _local(D, "08:05"),
            [{"trip_id": "T1", "stops": times(D, [("B", "08:14"), ("C", "08:24")])}],
        )
        frame = self._build()
        a, b, c = (self._row(frame, "T1", s) for s in (1, 2, 3))
        self.assertEqual(a["last_arrival_delay"], 120)
        self.assertEqual(a["prediction_count"], 1)
        self.assertEqual(b["first_arrival_delay"], 120)
        self.assertEqual(b["last_arrival_delay"], 240)
        self.assertEqual(b["last_departure_delay"], 180)  # scheduled departure 08:11
        self.assertEqual(
            c["last_predicted_arrival"], int(_local(D, "08:24").timestamp())
        )
        self.assertTrue(a["observed"])
        self.assertFalse(b["observed"])

    def test_trip_without_id_matched_by_route_and_start(self):
        self.feed.fetch(
            _local(D, "07:58"),
            [
                {
                    "route_id": "R1",
                    "start_time": "08:00:00",
                    "start_date": D.strftime("%Y%m%d"),
                    "stops": [{"seq": 1, "arrival_delay": 45}],
                }
            ],
        )
        frame = self._build()
        self.assertEqual(self._row(frame, "T1", 1)["last_arrival_delay"], 45)
        self.assertEqual(frame.height, 3)

    def test_unknown_added_trip_kept(self):
        self.feed.fetch(
            _local(D, "12:00"),
            [
                {
                    "trip_id": "X9",
                    "route_id": "R9",
                    "start_date": D.strftime("%Y%m%d"),
                    "relationship": "ADDED",
                    "stops": [
                        {"seq": 1, "stop": "A", "arrival_time": _local(D, "12:10")}
                    ],
                }
            ],
        )
        frame = self._build()
        row = self._row(frame, "X9", 1)
        self.assertEqual(row["trip_schedule_relationship"], "ADDED")
        self.assertIsNone(row["scheduled_arrival"])
        self.assertEqual(
            row["last_predicted_arrival"], int(_local(D, "12:10").timestamp())
        )

    def test_same_stop_with_and_without_stop_id(self):
        day = D.strftime("%Y%m%d")
        self.feed.fetch(
            _local(D, "07:50"),
            [
                {
                    "trip_id": "T1",
                    "start_date": day,
                    "stops": [{"seq": 1, "stop": "A", "arrival_delay": 60}],
                }
            ],
        )
        self.feed.fetch(
            _local(D, "07:55"),
            [
                {
                    "trip_id": "T1",
                    "start_date": day,
                    "stops": [{"seq": 1, "arrival_delay": 120}],
                }
            ],
        )
        frame = self._build()
        self.assertEqual(frame.height, 3)
        a = self._row(frame, "T1", 1)
        self.assertEqual((a["first_arrival_delay"], a["last_arrival_delay"]), (60, 120))
        self.assertEqual(a["prediction_count"], 2)
        self.assertEqual(a["stop_id"], "A")

    def test_no_data_stops_propagation(self):
        self.feed.fetch(
            _local(D, "09:55"),
            [
                {
                    "trip_id": "T4",
                    "start_date": D.strftime("%Y%m%d"),
                    "stops": [
                        {"seq": 1, "departure_delay": 600},
                        {"seq": 2, "no_data": True},
                    ],
                }
            ],
        )
        frame = self._build()
        b, c = self._row(frame, "T4", 2), self._row(frame, "T4", 3)
        self.assertEqual(b["stop_schedule_relationship"], "NO_DATA")
        self.assertIsNone(b["last_arrival_delay"])
        self.assertIsNone(c["last_arrival_delay"])
        self.assertFalse(c["delay_propagated"])

    def test_frequency_trip_runs(self):
        day = D.strftime("%Y%m%d")
        self.feed.fetch(
            _local(D, "07:58"),
            [
                {
                    "trip_id": "F1",
                    "start_date": day,
                    "start_time": "08:00:00",
                    "stops": [{"seq": 2, "arrival_delay": 60}],
                },
                {
                    "trip_id": "F1",
                    "start_date": day,
                    "start_time": "09:00:00",
                    "stops": [{"seq": 2, "arrival_delay": 600}],
                },
            ],
        )
        frame = (
            self._build().filter(pl.col("stop_sequence") == 2).sort("trip_start_time")
        )
        self.assertEqual(frame["trip_start_time"].to_list(), ["08:00:00", "09:00:00"])
        self.assertEqual(frame["last_arrival_delay"].to_list(), [60, 600])
        self.assertEqual(
            frame["scheduled_arrival"].to_list(),
            [int(_local(D, "08:10").timestamp()), int(_local(D, "09:10").timestamp())],
        )

    def test_deduplicated_row_overlapping_the_window(self):
        # Unchanged from 04:00 to 07:58 (one row), trip without startDate at
        # 08:00: its window opens at 05:00
        self.feed.fetch(
            _local(D, "07:58"),
            [
                {
                    "trip_id": "T1",
                    "stops": [{"stop": "A", "arrival_time": _local(D, "08:01")}],
                }
            ],
            seen=(_local(D, "04:00"), _local(D, "07:58")),
        )
        frame = self._build()
        a = self._row(frame, "T1", 1)
        self.assertEqual(a["last_arrival_delay"], 60)

    def test_path_template_needs_service_folder(self):
        from pydantic import ValidationError

        from src.gtfs_rt_aggregator.config.models import OutputConfig

        with self.assertRaises(ValidationError):
            OutputConfig(
                trip_stop_events=True,
                path_template="{provider}/{start:%Y-%m-%d}/{service}_{start:%H}.parquet",
            )

    def test_no_trips(self):
        self.feed.fetch(
            _local(D + timedelta(days=1), "12:00"),
            [
                {
                    "trip_id": "T1",
                    "start_date": (D + timedelta(days=1)).strftime("%Y%m%d"),
                }
            ],
        )
        self.assertIsNone(self._build())


class TestTripStopEventsService(unittest.TestCase):
    """The daily job on filesystem storage: layout, readiness, Iceberg."""

    def setUp(self):
        import json

        from src.gtfs_rt_aggregator.config.models import (
            ApiConfig,
            GtfsRtConfig,
            IcebergConfig,
            OutputConfig,
            ProviderConfig,
            StaticConfig,
            StorageConfig,
        )
        from src.gtfs_rt_aggregator.storage.filesystem import FileSystemStorage

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = os.path.join(tmp.name, "out")
        self.storage = FileSystemStorage(out)
        self.config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={"base_directory": out}),
            providers=[
                ProviderConfig(
                    name="be",
                    timezone="Europe/Brussels",
                    realtime=[
                        ApiConfig(url="https://x.org/tu", services=["TripUpdate"])
                    ],
                    static=[StaticConfig(name="static", url="https://x.org/gtfs.zip")],
                )
            ],
            output=OutputConfig(compact_daily=True, trip_stop_events=True),
            iceberg=IcebergConfig(
                catalog_uri=f"sqlite:///{tmp.name}/catalog.db",
                services=["TripStopEvent"],
            ),
        )
        # Static version v1 as StaticService stores it
        _static(
            os.path.join(out, "be", "static", "v1"),
            {
                "T1": [
                    (1, "A", "08:00:00", "08:00:00"),
                    (2, "B", "08:10:00", "08:10:00"),
                ]
            },
            {"T1": "R1"},
        )
        tables = {
            n: f"be/static/v1/{n}.parquet" for n in ("stop_times", "trips", "calendar")
        }
        manifest = json.dumps({"version": "v1", "tables": tables}).encode()
        self.storage.save_bytes(manifest, "be/static/v1/manifest.json")
        self.storage.save_bytes(manifest, "be/static/latest.json")
        # One hourly TripUpdate file of D
        feed = _Feed(tmp.name)
        feed.fetch(
            _local(D, "07:50"),
            [
                {
                    "trip_id": "T1",
                    "start_date": D.strftime("%Y%m%d"),
                    "stops": [{"seq": 1, "arrival_delay": 60}],
                }
            ],
        )
        start = _local(D, "07:00")
        path = self.config.output.path_template.format(
            provider="be",
            service="TripUpdate",
            start=start,
            end=start + timedelta(hours=1),
        )
        self.storage.save_file(feed.files[0], path)

    def test_builds_once_ready_and_registers(self):
        from src.gtfs_rt_aggregator.aggregator.trip_stop_service import (
            TripStopEventsService,
        )
        from src.gtfs_rt_aggregator.sinks.iceberg import IcebergSink

        service = TripStopEventsService(self.config, {"global": self.storage})
        after = D + timedelta(days=1)
        end = after + timedelta(days=1)
        self.assertEqual(service.run_once("be", now=_local(after, "23:00")), [])
        self.assertEqual(service.run_once("be", now=_local(end, "01:30")), [D])
        path = service.output_path("be", D, TZ)
        self.assertEqual(
            path, f"provider=be/service=TripStopEvent/date={D}/day.parquet"
        )
        table = pq.read_table(os.path.join(self.storage.base_directory, path))
        self.assertEqual(table.num_rows, 2)
        self.assertEqual(table.schema.field("trip_id").type, "string")
        # Built once
        self.assertEqual(service.run_once("be", now=_local(end, "03:00")), [])

        sink = IcebergSink(self.config, {"global": self.storage})
        self.assertEqual(sink.sync(days_back=None), 1)


if __name__ == "__main__":
    unittest.main()
