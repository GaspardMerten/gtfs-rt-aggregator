import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.gtfs_rt_aggregator.punctuality import COLUMNS, final_calls, storage_timetable
from tests.mocks import MockStorageInterface

DAY = "2026-10-05"
TZ = "Europe/Brussels"
# Noon minus 12 h of 5 Oct 2026 in Brussels (CEST): 4 Oct 22:00 UTC
BASE = datetime(2026, 10, 4, 22, tzinfo=timezone.utc).timestamp()
FETCH = datetime(2026, 10, 5, 7, 30, tzinfo=timezone.utc)

event = pa.struct([pa.field("delay", pa.int32()), pa.field("time", pa.int64())])
update = pa.struct([pa.field("stopSequence", pa.int64()), pa.field("stopId", pa.string()),
                    pa.field("arrival", event), pa.field("departure", event),
                    pa.field("scheduleRelationship", pa.string())])
TU_SCHEMA = pa.schema([
    ("date", pa.date32()), ("fetchTime", pa.timestamp("us", tz="UTC")), ("staticVersion", pa.string()),
    ("trip_tripId", pa.string()), ("trip_startDate", pa.string()), ("trip_startTime", pa.string()),
    ("trip_routeId", pa.string()), ("trip_scheduleRelationship", pa.string()),
    ("stopTimeUpdate", pa.list_(update)),
])


def at(hours: float) -> int:
    """Unix seconds of a GTFS time (hours) on the service day."""
    return int(BASE + hours * 3600)


def stu(seq=None, stop=None, arr_delay=None, arr_time=None, dep_delay=None, dep_time=None, status=None):
    return {"stopSequence": seq, "stopId": stop, "arrival": {"delay": arr_delay, "time": arr_time},
            "departure": {"delay": dep_delay, "time": dep_time}, "scheduleRelationship": status}


def trip_row(trip, updates, status=None, version="v1", fetch=FETCH):
    return {"date": date(2026, 10, 5), "fetchTime": fetch, "staticVersion": version, "trip_tripId": trip,
            "trip_startDate": "20261005", "trip_startTime": None, "trip_routeId": None,
            "trip_scheduleRelationship": status, "stopTimeUpdate": updates}


def stop_times(rows):
    """rows: (trip, seq, stop, hours, commercial)"""
    ms = [int(h * 3_600_000) for _, _, _, h, _ in rows]
    return pa.table({
        "trip_id": [r[0] for r in rows], "stop_sequence": pa.array([r[1] for r in rows], pa.int32()),
        "stop_id": [r[2] for r in rows],
        "arrival_time": pa.array(ms, pa.duration("ms")), "departure_time": pa.array(ms, pa.duration("ms")),
        "pickup_type": pa.array([0 if r[4] else 1 for r in rows], pa.int8()),
        "drop_off_type": pa.array([0 if r[4] else 1 for r in rows], pa.int8()),
    })


class _Case(unittest.TestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())

    def run_calls(self, rows, versions):
        """rows: TripUpdate rows; versions: {version: {table: pa.Table}}"""
        path = self.dir / "tu.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=TU_SCHEMA), path)

        def timetable(version, table, dest):
            if table not in versions.get(version, {}):
                return False
            pq.write_table(versions[version][table], dest)
            return True

        calls, stats = final_calls([path], DAY, TZ, timetable, self.dir / "work", default_version="v1")
        self.assertEqual(calls.column_names, COLUMNS)
        return calls.to_pylist(), stats

    @staticmethod
    def of(calls, trip):
        return sorted((c for c in calls if c["trip_id"] == trip), key=lambda c: (c["stop_sequence"] is None, c["stop_sequence"]))


class TestMatching(_Case):
    def test_by_stop_propagated_and_non_commercial(self):
        tt = {"v1": {"stop_times": stop_times([("T1", 1, "A", 8, True), ("T1", 2, "B", 8 + 1 / 6, False),
                                               ("T1", 3, "C", 8 + 2 / 6, True), ("T1", 4, "D", 8.5, True)])}}
        calls, stats = self.run_calls([trip_row("T1", [stu(stop="A", dep_delay=60), stu(stop="C", arr_delay=120)])], tt)
        rows = self.of(calls, "T1")
        # B is not commercial: not propagated to
        self.assertEqual([(r["stop_id"], r["arrival_delay"], r["delay_source"]) for r in rows],
                         [("A", None, "feed"), ("C", 120, "feed"), ("D", 120, "propagated")])
        self.assertEqual(rows[0]["departure_delay"], 60)
        self.assertEqual(rows[2]["predicted_arrival"], at(8.5) + 120)
        self.assertEqual(stats["propagated_calls"], 1)
        self.assertEqual(stats["updates_matched"], 1.0)

    def test_times_only_and_wrong_day(self):
        tt = {"v1": {"stop_times": stop_times([("T1", 1, "A", 8, True), ("T1", 2, "C", 9, True)])}}
        rows = self.of(self.run_calls([trip_row("T1", [
            stu(stop="A", dep_time=at(8) + 180),
            # Dated the day before
            stu(stop="C", arr_time=at(9) + 300 - 86400),
        ])], tt)[0], "T1")
        self.assertEqual(rows[0]["departure_delay"], 180)
        self.assertEqual(rows[1]["arrival_delay"], 300)
        self.assertEqual(rows[1]["predicted_arrival"], at(9) + 300)

    def test_calling_twice_at_one_stop(self):
        tt = {"v1": {"stop_times": stop_times([("L", 1, "X", 9, True), ("L", 2, "Y", 9.5, True), ("L", 3, "X", 10, True)])}}
        rows = self.of(self.run_calls([trip_row("L", [
            stu(stop="X", dep_time=at(9) + 120), stu(stop="X", arr_time=at(10) + 300),
        ])], tt)[0], "L")
        self.assertEqual([(r["stop_sequence"], r["delay_source"]) for r in rows],
                         [(1, "feed"), (2, "propagated"), (3, "feed")])
        self.assertEqual(rows[0]["departure_delay"], 120)
        self.assertEqual(rows[2]["arrival_delay"], 300)

    def test_other_platform_of_the_station(self):
        stops = pa.table({"stop_id": ["A:1", "A:2", "A"], "parent_station": ["A", "A", None]})
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A:1", 8, True)]), "stops": stops}}
        rows = self.of(self.run_calls([trip_row("T", [stu(stop="A:2", dep_delay=90)])], tt)[0], "T")
        self.assertEqual([(r["stop_id"], r["departure_delay"]) for r in rows], [("A:1", 90)])

    def test_commercial_rank_numbering(self):
        # The feed numbers only the calls where passengers can board or alight
        # Calls an hour apart, S2 not commercial: the feed's 2 is S3, its 3 is S4
        tt = {"v1": {"stop_times": stop_times([(f"T{i}", s, f"S{s}", 7 + s, s != 2) for i in range(3) for s in range(1, 5)])}}
        rows = [trip_row(f"T{i}", [stu(seq=1, dep_delay=0, dep_time=at(8)), stu(seq=2, arr_delay=60, arr_time=at(10) + 60),
                                   stu(seq=3, arr_delay=120, arr_time=at(11) + 120)]) for i in range(3)]
        calls, stats = self.run_calls(rows, tt)
        self.assertEqual(stats["stop_sequence_mode"], "commercial")
        self.assertEqual([(r["stop_id"], r["arrival_delay"]) for r in self.of(calls, "T0")],
                         [("S1", None), ("S3", 60), ("S4", 120)])

    def test_cancelled_and_added(self):
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True), ("T", 2, "B", 9, True)])}}
        calls, stats = self.run_calls([
            trip_row("T", [], status="CANCELED"),
            trip_row("EXTRA", [stu(seq=1, stop="Z", dep_time=at(8))], status="ADDED"),
        ], tt)
        cancelled = self.of(calls, "T")
        self.assertEqual([(r["stop_id"], r["arrival_delay"], r["trip_schedule_relationship"]) for r in cancelled],
                         [("A", None, "CANCELED"), ("B", None, "CANCELED")])
        extra = self.of(calls, "EXTRA")
        self.assertEqual([(r["stop_id"], r["scheduled_departure"], r["predicted_departure"]) for r in extra], [("Z", None, at(8))])
        self.assertEqual(stats["trips_added"], 0.5)

    def test_trip_renamed_after_the_next_timetable_year(self):
        trips = pa.table({"trip_id": ["IC:1813:20261211", "IC:1999:20261211"], "route_id": ["R1", "R2"],
                          "service_id": ["S", "S"]})
        calendar_dates = pa.table({"service_id": ["S"], "date": pa.array([date(2026, 10, 5)], pa.date32()),
                                   "exception_type": pa.array([1], pa.int8())})
        tt = {"v1": {"stop_times": stop_times([("IC:1813:20261211", 1, "A", 8, True)]), "trips": trips,
                     "calendar_dates": calendar_dates}}
        calls, stats = self.run_calls([trip_row("IC:1813:20271210", [stu(stop="A", dep_delay=30)])], tt)
        rows = self.of(calls, "IC:1813:20271210")
        self.assertEqual([(r["stop_id"], r["departure_delay"], r["route_id"]) for r in rows], [("A", 30, "R1")])
        self.assertEqual(stats["trips_matched_by_id_before_date"], 1)

    def test_trip_dated_by_its_running_day(self):
        # es-renfe-ld: the live id ends in the running day, the timetable's in the start of each period
        trips = pa.table({"trip_id": ["0029312026-10-01", "0029312026-10-04"], "route_id": ["R1", "R2"],
                          "service_id": ["P1", "P2"]})
        calendar = pa.table({"service_id": ["P1", "P2"], **{d: pa.array([1, 1], pa.int8()) for d in
                             ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")},
                             "start_date": pa.array([date(2026, 10, 1), date(2026, 10, 4)], pa.date32()),
                             "end_date": pa.array([date(2026, 10, 3), date(2026, 10, 9)], pa.date32())})
        tt = {"v1": {"stop_times": stop_times([("0029312026-10-01", 1, "A", 7, True), ("0029312026-10-04", 1, "A", 8, True)]),
                     "trips": trips, "calendar": calendar}}
        calls, stats = self.run_calls([trip_row("0029312026-10-05", [stu(stop="A", dep_delay=30)])], tt)
        rows = self.of(calls, "0029312026-10-05")
        self.assertEqual([(r["stop_id"], r["departure_delay"], r["route_id"]) for r in rows], [("A", 30, "R2")])
        self.assertEqual(stats["trips_matched_by_id_before_date"], 1)

    def test_trip_only_in_a_newer_version(self):
        tt = {"v1": {"stop_times": stop_times([("OTHER", 1, "A", 8, True)])},
              "v2": {"stop_times": stop_times([("T", 1, "A", 8, True)])}}
        calls, _ = self.run_calls([trip_row("T", [stu(stop="A", dep_delay=60)], version="v1"),
                                   trip_row("OTHER", [stu(stop="A", dep_delay=0)], version="v2")], tt)
        self.assertEqual([(r["stop_id"], r["departure_delay"], r["delay_source"]) for r in self.of(calls, "T")],
                         [("A", 60, "feed")])

    def test_last_value_wins(self):
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True)])}}
        later = datetime(2026, 10, 5, 7, 45, tzinfo=timezone.utc)
        calls, _ = self.run_calls([trip_row("T", [stu(stop="A", dep_delay=60)]),
                                   trip_row("T", [stu(stop="A", dep_delay=240)], fetch=later)], tt)
        row = self.of(calls, "T")[0]
        self.assertEqual(row["departure_delay"], 240)
        self.assertEqual(row["observed_at"], later.timestamp())

    def test_no_timetable(self):
        calls, stats = self.run_calls([trip_row("T", [stu(seq=1, stop="A", dep_delay=60)])], {})
        self.assertEqual([(r["stop_id"], r["departure_delay"], r["scheduled_departure"]) for r in calls], [("A", 60, None)])
        self.assertEqual(stats["trips_in_timetable"], 0.0)


class TestStorageTimetable(unittest.TestCase):
    def test_reads_stored_versions(self):
        storage = MockStorageInterface()
        out = Path(tempfile.mkdtemp())
        pq.write_table(pa.table({"stop_id": ["A"]}), out / "stops.parquet")
        storage.save_file(str(out / "stops.parquet"), "be/static/v1/stops.parquet")
        storage.save_bytes(json.dumps({"version": "v1", "tables": {"stops": "be/static/v1/stops.parquet"}}).encode(),
                           "be/static/v1/manifest.json")
        fetch = storage_timetable(storage, "be")
        self.assertTrue(fetch("v1", "stops", out / "got.parquet"))
        self.assertEqual(pq.read_table(out / "got.parquet")["stop_id"].to_pylist(), ["A"])
        self.assertFalse(fetch("v1", "trips", out / "no.parquet"))
        self.assertFalse(fetch("v9", "stops", out / "no.parquet"))


class TestTies(_Case):
    def test_one_fetch_sending_a_trip_twice(self):
        # Same fetch, two entities for one trip: the later row wins, every time
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True), ("T", 2, "B", 9, True)])}}
        rows = [trip_row("T", [stu(seq=1, stop="A", dep_delay=60), stu(seq=2, stop="B", arr_delay=60)], status="SCHEDULED"),
                trip_row("T", [stu(seq=1, stop="A", dep_delay=300), stu(seq=2, stop="B", arr_delay=300)], status="CANCELED")]
        for _ in range(3):
            calls, _ = self.run_calls(rows, tt)
            self.assertEqual({(r["stop_id"], r["trip_schedule_relationship"]) for r in calls}, {("A", "CANCELED"), ("B", "CANCELED")})
        calls, _ = self.run_calls(rows[:1] + [trip_row("T", [stu(seq=1, stop="A", dep_delay=300)])], tt)
        self.assertEqual(self.of(calls, "T")[0]["departure_delay"], 300)

    def test_update_numbered_as_the_call_wins(self):
        # Two updates of one fetch at stop X match call 1: the one numbered 1 is kept, wherever it is in the list
        tt = {"v1": {"stop_times": stop_times([("L", 1, "X", 9, True), ("L", 2, "Y", 9.1, True)])}}
        for updates in ([stu(seq=1, stop="X", dep_delay=60), stu(seq=7, stop="X", dep_delay=600)],
                        [stu(seq=7, stop="X", dep_delay=600), stu(seq=1, stop="X", dep_delay=60)]):
            calls, _ = self.run_calls([trip_row("L", updates)], tt)
            self.assertEqual(self.of(calls, "L")[0]["departure_delay"], 60)

    def test_row_without_fetch_time_never_wins(self):
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True), ("T", 2, "B", 9, True)])}}
        calls, _ = self.run_calls([trip_row("T", [], status="CANCELED"), trip_row("T", [], status="SCHEDULED", fetch=None)], tt)
        self.assertEqual({r["trip_schedule_relationship"] for r in calls}, {"CANCELED"})


class TestInputs(_Case):
    def test_no_files_gives_the_same_columns_and_types(self):
        empty, stats = final_calls([], DAY, TZ, lambda *a: False, self.dir / "work")
        self.assertEqual((empty.num_rows, stats), (0, {}))
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True)])}}
        path = self.dir / "tu.parquet"
        pq.write_table(pa.Table.from_pylist([trip_row("T", [stu(seq=1, stop="A", dep_delay=60)])], schema=TU_SCHEMA), path)
        full, _ = final_calls([path], DAY, TZ, lambda v, t, d: t in tt["v1"] and (pq.write_table(tt["v1"][t], d) or True),
                              self.dir / "work", default_version="v1")
        self.assertEqual(empty.schema, full.schema)

    def test_bad_arguments(self):
        with self.assertRaises(ValueError):
            final_calls([], "24/10/2026", TZ, lambda *a: False, self.dir / "work")
        with self.assertRaises(ValueError):
            final_calls([], DAY, TZ, lambda *a: False, self.dir / "work", memory_limit="650MB'; SELECT 1; --")
        final_calls([], DAY.replace("-", ""), TZ, lambda *a: False, self.dir / "work", memory_limit="1.5GiB", threads=1)

    def test_no_version_warns(self):
        path = self.dir / "tu.parquet"
        pq.write_table(pa.Table.from_pylist([trip_row("T", [stu(seq=1, stop="A", dep_delay=60)], version=None)], schema=TU_SCHEMA), path)
        with self.assertLogs("src.gtfs_rt_aggregator.punctuality", "WARNING"):
            calls, _ = final_calls([path], DAY, TZ, lambda *a: False, self.dir / "work")
        self.assertEqual([(r["stop_id"], r["departure_delay"]) for r in calls.to_pylist()], [("A", 60)])

    def test_rows_come_sorted(self):
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True), ("T", 2, "B", 9, True), ("S", 1, "A", 8.5, True)])}}
        calls, _ = self.run_calls([trip_row("T", [stu(seq=1, stop="A", dep_delay=60)]), trip_row("S", [stu(seq=1, stop="A", dep_delay=0)])], tt)
        self.assertEqual([(r["trip_id"], r["stop_sequence"]) for r in calls], [("S", 1), ("T", 1), ("T", 2)])

    def test_update_naming_the_stop_wins(self):
        # One fetch: B by stop (seq 5 in the feed's own numbering) and an update without stop numbered 2, which
        # the position numbering also gives call B: the update naming B is kept, wherever it is in the list
        tt = {"v1": {"stop_times": stop_times([("P", 1, "A", 8, True), ("P", 2, "B", 9, True), ("P", 3, "C", 10, True)])}}
        for updates in ([stu(seq=4, stop="A", dep_delay=10), stu(seq=5, stop="B", arr_delay=145), stu(seq=2, arr_delay=900)],
                        [stu(seq=2, arr_delay=900), stu(seq=4, stop="A", dep_delay=10), stu(seq=5, stop="B", arr_delay=145)]):
            calls, _ = self.run_calls([trip_row("P", updates)], tt)
            self.assertEqual([r["arrival_delay"] for r in self.of(calls, "P") if r["stop_id"] == "B"], [145])

    def test_files_of_incompatible_versions(self):
        a, b = self.dir / "a.parquet", self.dir / "b.parquet"
        pq.write_table(pa.table({"trip_tripId": ["T"], "fetchTime": pa.array([1], pa.uint64())}), a)
        pq.write_table(pa.table({"trip_tripId": ["T"], "fetchTime": pa.array([1], pa.timestamp("us", "UTC"))}), b)
        with self.assertRaisesRegex(ValueError, "schemas differ"):
            final_calls([a, b], DAY, TZ, lambda *a: False, self.dir / "work")


class TestCallingTwice(_Case):
    # A loop: A, B, C, then A again
    TT = {"v1": {"stop_times": stop_times([("L", 1, "A", 9, True), ("L", 2, "B", 9.2, True), ("L", 3, "C", 9.4, True),
                                           ("L", 4, "A", 9.6, True)])}}

    def delays(self, updates):
        calls, _ = self.run_calls([trip_row("L", updates)], self.TT)
        return [(r["stop_sequence"], r["arrival_delay"], r["delay_source"]) for r in self.of(calls, "L")]

    def test_delays_only(self):
        got = self.delays([stu(seq=1, stop="A", dep_delay=60), stu(seq=2, stop="B", arr_delay=120),
                           stu(seq=3, stop="C", arr_delay=180), stu(seq=4, stop="A", arr_delay=240)])
        self.assertEqual(got[3], (4, 240, "feed"))

    def test_first_calls_passed(self):
        got = self.delays([stu(seq=3, stop="C", arr_delay=180), stu(seq=4, stop="A", arr_delay=240)])
        self.assertEqual(got, [(3, 180, "feed"), (4, 240, "feed")])

    def test_times_only_late_beyond_half_the_gap(self):
        tt = {"v1": {"stop_times": stop_times([("X", 1, "X", 9, True), ("X", 2, "Y", 9.1, True), ("X", 3, "X", 9.2, True),
                                               ("X", 4, "Z", 9.3, True), ("X", 5, "X", 9.4, True)])}}
        late = lambda h: at(h + 0.25)
        calls, _ = self.run_calls([trip_row("X", [stu(seq=1, stop="X", dep_time=late(9)), stu(seq=3, stop="X", arr_time=late(9.2)),
                                                  stu(seq=5, stop="X", arr_time=late(9.4))])], tt)
        self.assertEqual({r["stop_sequence"]: r["arrival_delay"] or r["departure_delay"] for r in self.of(calls, "X") if r["stop_id"] == "X"},
                         {1: 900, 3: 900, 5: 900})


class TestPropagation(_Case):
    def test_stops_at_no_data(self):
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True), ("T", 2, "B", 9, True), ("T", 3, "C", 10, True),
                                               ("T", 4, "D", 11, True)])}}
        calls, _ = self.run_calls([trip_row("T", [stu(seq=1, stop="A", dep_delay=300), stu(seq=2, stop="B", status="NO_DATA")])], tt)
        self.assertEqual([(r["stop_id"], r["departure_delay"]) for r in self.of(calls, "T")], [("A", 300), ("B", None)])

    def test_goes_on_after_skipped(self):
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True), ("T", 2, "B", 9, True), ("T", 3, "C", 10, True)])}}
        calls, _ = self.run_calls([trip_row("T", [stu(seq=1, stop="A", dep_delay=300), stu(seq=2, stop="B", status="SKIPPED")])], tt)
        self.assertEqual([(r["stop_id"], r["departure_delay"], r["delay_source"]) for r in self.of(calls, "T")][2], ("C", 300, "propagated"))

    def test_skipped_stop_keeps_no_delay_and_is_not_carried(self):
        # The feed sends a time-looking delay at a skipped stop (+40 min): the stop keeps none, and the next
        # stop carries the last delay of a stop the train called at (+5 min)
        tt = {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True), ("T", 2, "B", 9, True), ("T", 3, "C", 10, True)])}}
        calls, _ = self.run_calls([trip_row("T", [stu(seq=1, stop="A", dep_delay=300),
                                                  stu(seq=2, stop="B", arr_delay=2400, dep_delay=2400, status="SKIPPED")])], tt)
        rows = [(r["stop_id"], r["arrival_delay"], r["predicted_arrival"], r["stop_schedule_relationship"]) for r in self.of(calls, "T")]
        self.assertEqual(rows[1], ("B", None, None, "SKIPPED"))
        self.assertEqual(self.of(calls, "T")[2]["arrival_delay"], 300)

    def test_cancelled_train_outside_the_timetable_keeps_no_delay(self):
        calls, _ = self.run_calls([trip_row("GONE", [stu(seq=1, stop="A", arr_delay=900)], status="CANCELED")],
                                  {"v1": {"stop_times": stop_times([("T", 1, "A", 8, True)])}})
        self.assertEqual([(r["stop_id"], r["arrival_delay"]) for r in self.of(calls, "GONE")], [("A", None)])


class TestVersions(_Case):
    def test_each_row_names_its_timetable_version(self):
        # Ids changed between versions (a timetable release mid-day): each train names the version it matched
        tt = {"v1": {"stop_times": stop_times([("OLD", 1, "A", 8, True)])},
              "v2": {"stop_times": stop_times([("NEW", 1, "A", 9, True)])}}
        calls, _ = self.run_calls([trip_row("OLD", [stu(seq=1, stop="A", arr_delay=60)], version="v1"),
                                   trip_row("NEW", [stu(seq=1, stop="A", arr_delay=60)], version="v2")], tt)
        self.assertEqual({r["trip_id"]: r["timetable_version"] for r in calls}, {"OLD": "v1", "NEW": "v2"})


class TestUndated(_Case):
    # Night train: A 23:30, B 24:00, C 24:30; the feed sends no startDate
    TT = {"v1": {"stop_times": stop_times([("N", 1, "A", 23.5, True), ("N", 2, "B", 24, True), ("N", 3, "C", 24.5, True)])}}

    @staticmethod
    def row(updates, local_day, utc):
        return dict(trip_row("N", updates, fetch=utc), trip_startDate=None, date=local_day)

    def test_fetch_after_midnight_counts_for_the_previous_day(self):
        rows = [self.row([stu(seq=1, stop="A", dep_delay=60)], date(2026, 10, 5), datetime(2026, 10, 5, 21, 40, tzinfo=timezone.utc)),
                self.row([stu(seq=3, stop="C", arr_delay=600)], date(2026, 10, 6), datetime(2026, 10, 5, 22, 20, tzinfo=timezone.utc))]
        calls, _ = self.run_calls(rows, self.TT)
        self.assertEqual([(r["stop_id"], r["arrival_delay"], r["delay_source"]) for r in self.of(calls, "N")][2], ("C", 600, "feed"))

    def test_last_nights_run_is_left_out(self):
        rows = [self.row([stu(seq=3, stop="C", arr_delay=900)], date(2026, 10, 5), datetime(2026, 10, 4, 22, 20, tzinfo=timezone.utc)),
                self.row([stu(seq=1, stop="A", dep_delay=60)], date(2026, 10, 5), datetime(2026, 10, 5, 21, 40, tzinfo=timezone.utc))]
        calls, _ = self.run_calls(rows, self.TT)
        self.assertEqual([(r["stop_id"], r["arrival_delay"], r["delay_source"]) for r in self.of(calls, "N")][2], ("C", 60, "propagated"))

    def test_files_without_date_column(self):
        # Before 0.6.0: no date column; the next day's run of a daily trip must not count
        tt = {"v1": {"stop_times": stop_times([("D", 1, "A", 8, True), ("D", 2, "B", 9, True)])}}
        schema = TU_SCHEMA.remove(TU_SCHEMA.get_field_index("date"))
        rows = [dict(trip_row("D", [stu(seq=2, stop="B", arr_delay=d)], fetch=f), trip_startDate=None)
                for d, f in ((60, datetime(2026, 10, 5, 6, 50, tzinfo=timezone.utc)), (1200, datetime(2026, 10, 6, 6, 50, tzinfo=timezone.utc)))]
        for r in rows:
            del r["date"]
        path = self.dir / "old.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        calls, _ = final_calls([path], DAY, TZ, lambda v, t, d: t in tt["v1"] and (pq.write_table(tt["v1"][t], d) or True),
                               self.dir / "work", default_version="v1")
        self.assertEqual([r["arrival_delay"] for r in calls.to_pylist() if r["stop_id"] == "B"], [60])
