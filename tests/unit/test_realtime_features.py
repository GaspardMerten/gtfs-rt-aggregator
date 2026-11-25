"""Skipping unchanged fetches, filters, extra columns, status files and retries."""

import io
import json
import os
import unittest
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    FilterConfig,
    GtfsRtConfig,
    ProviderConfig,
    StaticConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.fetcher.gtfs_rt import GtfsRtFetcher
from src.gtfs_rt_aggregator.fetcher.service import FetcherService
from src.gtfs_rt_aggregator.utils.http import get_bytes
from tests.mocks import MockStorageInterface

DATA = os.path.join(os.path.dirname(__file__), "..", "data")
URL = "https://example.org/vehicle_positions.pb"


def _feed_bytes():
    with open(os.path.join(DATA, "vehicle_positions.pb"), "rb") as f:
        return f.read()


def _parquet(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


class _FetcherTest(unittest.TestCase):
    def _service(self, api: ApiConfig, static=None):
        self.storage = MockStorageInterface()
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem"),
            providers=[
                ProviderConfig(
                    name="p", timezone="UTC", realtime=[api], static=static or []
                )
            ],
        )
        service = FetcherService(config, {"global": self.storage})
        return service

    def _run(self, service, data=None):
        with patch.object(
            GtfsRtFetcher, "fetch_feed", return_value=data or _feed_bytes()
        ):
            service.run_once("p", URL, ["VehiclePosition"], "UTC")

    def _individual(self):
        return sorted(
            p for p in self.storage.list_paths("p/VehiclePosition/individual/")
        )

    def _status(self):
        (path,) = self.storage.list_paths("p/_status/")
        return json.loads(self.storage.get_bytes(path))


class TestSkipUnchanged(_FetcherTest):
    def test_unchanged_fetch_not_stored(self):
        service = self._service(ApiConfig(url=URL, services=["VehiclePosition"]))
        self._run(service)
        self.assertEqual(len(self._individual()), 1)

        self._run(service)
        self.assertEqual(len(self._individual()), 1)
        status = self._status()
        self.assertTrue(status["unchanged"])
        self.assertEqual(status["entity_count"], 3549)
        self.assertEqual(status["feed_timestamp"], 1742550861)
        self.assertIn("last_success", status)

    def test_same_entities_other_order_or_timestamp_not_stored(self):
        service = self._service(ApiConfig(url=URL, services=["VehiclePosition"]))
        self._run(service)
        message = GtfsRtFetcher.parse_message(_feed_bytes())
        message.header.timestamp += 60
        entities = list(message.entity)[::-1]
        del message.entity[:]
        message.entity.extend(entities)
        self._run(service, message.SerializeToString())
        self.assertEqual(len(self._individual()), 1)

    def test_changed_entity_stored(self):
        service = self._service(ApiConfig(url=URL, services=["VehiclePosition"]))
        self._run(service)
        message = GtfsRtFetcher.parse_message(_feed_bytes())
        message.entity[0].vehicle.position.latitude += 0.01
        with patch(
            "src.gtfs_rt_aggregator.fetcher.service.format_file_time",
            return_value="2026-01-01_00-00-01Z",
        ):
            self._run(service, message.SerializeToString())
        self.assertEqual(len(self._individual()), 2)

    def test_skip_disabled(self):
        service = self._service(
            ApiConfig(url=URL, services=["VehiclePosition"], skip_unchanged=False)
        )
        with patch(
            "src.gtfs_rt_aggregator.fetcher.service.format_file_time",
            side_effect=["2026-01-01_00-00-01Z", "2026-01-01_00-00-02Z"],
        ):
            self._run(service)
            self._run(service)
        self.assertEqual(len(self._individual()), 2)

    def test_extra_columns(self):
        service = self._service(ApiConfig(url=URL, services=["VehiclePosition"]))
        self._run(service)
        table = pq.read_table(io.BytesIO(self.storage.get_bytes(self._individual()[0])))
        self.assertEqual(
            {t.timestamp() for t in table["feedTimestamp"].to_pylist()}, {1742550861}
        )
        self.assertEqual(
            table.schema.field("fetchTime").type, pa.timestamp("us", tz="UTC")
        )
        self.assertEqual(set(table["provider"].to_pylist()), {"p"})
        self.assertEqual(len(set(table["date"].to_pylist())), 1)
        # No unsigned types anywhere (Iceberg has none)
        self.assertNotIn("uint", str(table.schema))
        self.assertEqual(set(table["staticVersion"].to_pylist()), {None})
        self.assertEqual(table["contentHash"].null_count, 0)
        self.assertEqual(len(set(table["contentHash"].to_pylist())), 3549)

    def test_failure_keeps_last_success_in_status(self):
        service = self._service(
            ApiConfig(url=URL + "?key=secret", services=["VehiclePosition"])
        )
        with patch.object(GtfsRtFetcher, "fetch_feed", return_value=_feed_bytes()):
            service.run_once("p", URL + "?key=secret", ["VehiclePosition"], "UTC")
        with patch.object(
            GtfsRtFetcher, "fetch_feed", side_effect=requests.ConnectionError("TLS")
        ):
            service.run_once("p", URL + "?key=secret", ["VehiclePosition"], "UTC")
        status = self._status()
        self.assertIn("last_success", status)
        self.assertIn("TLS", status["last_error"])
        # The query string can hold an API key
        self.assertNotIn("secret", json.dumps(status))


class TestRobustness(_FetcherTest):
    def test_failed_store_is_retried(self):
        service = self._service(ApiConfig(url=URL, services=["VehiclePosition"]))
        with patch.object(self.storage, "save_bytes", side_effect=IOError("disk full")):
            with patch.object(GtfsRtFetcher, "fetch_feed", return_value=_feed_bytes()):
                service.run_once("p", URL, ["VehiclePosition"], "UTC")
        self._run(service)
        self.assertEqual(len(self._individual()), 1)

    def test_static_lookup_error_does_not_fail_fetch(self):
        api = ApiConfig(url=URL, services=["VehiclePosition"])
        service = self._service(api, [StaticConfig(url="https://example.org/gtfs.zip")])
        with patch.object(
            self.storage, "file_exists", side_effect=IOError("storage down")
        ):
            self._run(service)
        self.assertEqual(len(self._individual()), 1)


class TestFilter(_FetcherTest):
    def _static(self, service_version="2026-01-01_00-00-00Z"):
        """Static version where route 125327 is rail (2) and the others buses (3)."""
        message = GtfsRtFetcher.parse_message(_feed_bytes())
        trips = {
            (e.vehicle.trip.trip_id, e.vehicle.trip.route_id) for e in message.entity
        }
        routes = sorted({r for _, r in trips})
        base = "p/static"
        tables = {
            "routes": f"{base}/{service_version}/routes.parquet",
            "trips": f"{base}/{service_version}/trips.parquet",
        }
        self.storage.save_bytes(
            _parquet(
                pa.table(
                    {
                        "route_id": routes,
                        "route_type": pa.array(
                            [
                                2 if r == "125327" else 700 if r == "126323" else 3
                                for r in routes
                            ],
                            pa.int16(),
                        ),
                    }
                )
            ),
            tables["routes"],
        )
        self.storage.save_bytes(
            _parquet(
                pa.table(
                    {
                        # Realtime ids without route_id are matched via trips
                        "trip_id": [t for t, _ in sorted(trips)],
                        "route_id": [r for _, r in sorted(trips)],
                    }
                )
            ),
            tables["trips"],
        )
        self.storage.save_bytes(
            json.dumps({"version": service_version, "tables": tables}).encode(),
            f"{base}/latest.json",
        )
        return sum(1 for _, r in trips if r == "125327"), sum(
            1 for _, r in trips if r == "126323"
        )

    def _rows(self):
        return pq.read_table(io.BytesIO(self.storage.get_bytes(self._individual()[0])))

    def test_route_types(self):
        api = ApiConfig(
            url=URL,
            services=["VehiclePosition"],
            filter=FilterConfig(route_types=[2, "100-199"]),
        )
        service = self._service(api, [StaticConfig(url="https://example.org/gtfs.zip")])
        rail, _ = self._static()
        with patch("tempfile.gettempdir", return_value=self.id_tmp()):
            self._run(service)
        rows = self._rows()
        self.assertEqual(set(rows["trip_routeId"].to_pylist()), {"125327"})
        self.assertEqual(rows.num_rows, rail)
        self.assertEqual(
            set(rows["staticVersion"].to_pylist()), {"2026-01-01_00-00-00Z"}
        )
        status = self._status()
        self.assertEqual(status["kept_count"], rail)
        # Every trip is in the static version: none dropped as unknown
        self.assertEqual(status["dropped_count"], status["entity_count"] - rail)
        self.assertEqual(status["dropped_unknown"], 0)

    def test_extended_route_type(self):
        api = ApiConfig(
            url=URL,
            services=["VehiclePosition"],
            filter=FilterConfig(route_types=["700-799"]),
        )
        service = self._service(api, [StaticConfig(url="https://example.org/gtfs.zip")])
        _, buses = self._static()
        with patch("tempfile.gettempdir", return_value=self.id_tmp()):
            self._run(service)
        rows = self._rows()
        self.assertEqual(set(rows["trip_routeId"].to_pylist()), {"126323"})
        self.assertEqual(rows.num_rows, buses)

    def _filtered_static(self, other_modes=True, unknown_route="126323"):
        """
        Static version stored rail-only (route_types [2]): 125327 is rail; the
        side table lists the trips and routes of the other modes, except
        unknown_route's, which the static feed does not know (a new timetable).
        """
        message = GtfsRtFetcher.parse_message(_feed_bytes())
        trips = sorted(
            {(e.vehicle.trip.trip_id, e.vehicle.trip.route_id) for e in message.entity}
        )
        version = "2026-01-01_00-00-00Z"
        base = f"p/static/{version}"
        tables = {"routes": f"{base}/routes.parquet", "trips": f"{base}/trips.parquet"}
        rail = [(t, r) for t, r in trips if r == "125327"]
        others = [(t, r) for t, r in trips if r not in ("125327", unknown_route)]
        self.storage.save_bytes(
            _parquet(pa.table({"route_id": ["125327"], "route_type": pa.array([2], pa.int16())})),
            tables["routes"],
        )
        self.storage.save_bytes(
            _parquet(
                pa.table({"trip_id": [t for t, _ in rail], "route_id": [r for _, r in rail]})
            ),
            tables["trips"],
        )
        manifest = {"version": version, "tables": tables, "route_types": [2]}
        if other_modes:
            manifest["other_modes"] = f"{base}/_other_modes.parquet"
            self.storage.save_bytes(
                _parquet(
                    pa.table(
                        {
                            # Trip ids only: vehicles carrying a route id are matched by it too
                            "trip_id": [t for t, _ in others] + [None],
                            "route_id": [r for _, r in others] + ["route-without-trips"],
                            "route_type": pa.array([3] * len(others) + [0], pa.int32()),
                        }
                    )
                ),
                manifest["other_modes"],
            )
        self.storage.save_bytes(json.dumps(manifest).encode(), "p/static/latest.json")
        return len(rail), sum(1 for _, r in trips if r == unknown_route)

    def test_unknown_trips_kept_with_other_modes(self):
        api = ApiConfig(
            url=URL, services=["VehiclePosition"], filter=FilterConfig(route_types=[2])
        )
        service = self._service(api, [StaticConfig(url="https://example.org/gtfs.zip")])
        rail, unknown = self._filtered_static()
        self.assertGreater(unknown, 0)
        with patch("tempfile.gettempdir", return_value=self.id_tmp()):
            self._run(service)
        rows = self._rows()
        # Buses known from the side table dropped, the trips nobody knows kept
        self.assertEqual(set(rows["trip_routeId"].to_pylist()), {"125327", "126323"})
        self.assertEqual(rows.num_rows, rail + unknown)
        status = self._status()
        self.assertEqual(status["kept_unknown"], unknown)
        self.assertEqual(status["dropped_unknown"], 0)

    def test_unknown_trips_dropped_without_other_modes(self):
        # A filtered version stored before 0.13.0: unknown trips look like other modes
        api = ApiConfig(
            url=URL, services=["VehiclePosition"], filter=FilterConfig(route_types=[2])
        )
        service = self._service(api, [StaticConfig(url="https://example.org/gtfs.zip")])
        rail, _ = self._filtered_static(other_modes=False)
        with patch("tempfile.gettempdir", return_value=self.id_tmp()):
            self._run(service)
        rows = self._rows()
        self.assertEqual(set(rows["trip_routeId"].to_pylist()), {"125327"})
        status = self._status()
        self.assertEqual(status["kept_unknown"], 0)
        self.assertEqual(status["dropped_unknown"], status["entity_count"] - rail)

    def test_other_modes_from_the_disk_cache(self):
        # A second worker reads the resolution another one wrote
        from src.gtfs_rt_aggregator.fetcher import filter as filter_module

        api = ApiConfig(
            url=URL, services=["VehiclePosition"], filter=FilterConfig(route_types=[2])
        )
        service = self._service(api, [StaticConfig(url="https://example.org/gtfs.zip")])
        self._filtered_static()
        tmp = self.id_tmp()
        tables = {
            "routes": "p/static/2026-01-01_00-00-00Z/routes.parquet",
            "trips": "p/static/2026-01-01_00-00-00Z/trips.parquet",
            filter_module.OTHER_MODES: "p/static/2026-01-01_00-00-00Z/_other_modes.parquet",
        }
        with patch("tempfile.gettempdir", return_value=tmp):
            first = filter_module.build_filter(api.filter, self.storage, tables, "k")
            filter_module._MEMO.clear()
            with patch.object(filter_module, "_resolve", side_effect=AssertionError):
                second = filter_module.build_filter(api.filter, self.storage, tables, "k")
        self.assertEqual(second.other_routes, first.other_routes)
        self.assertEqual(list(second.other_trips), list(first.other_trips))
        self.assertIn("route-without-trips", second.other_routes)
        # Adding the side table to the same version resolves again
        with patch("tempfile.gettempdir", return_value=tmp):
            old = dict(tables)
            del old[filter_module.OTHER_MODES]
            self.assertFalse(
                filter_module.build_filter(api.filter, self.storage, old, "k").knows_other_modes
            )

    def test_trip_ids_without_static(self):
        api = ApiConfig(
            url=URL,
            services=["VehiclePosition"],
            filter=FilterConfig(trip_ids=["280007323", "280007326"]),
        )
        service = self._service(api)
        self._run(service)
        self.assertEqual(
            sorted(self._rows()["trip_tripId"].to_pylist()), ["280007323", "280007326"]
        )

    def test_no_static_version_yet_stores_nothing(self):
        # Stored unfiltered, the fetch would archive the rows the filter drops
        api = ApiConfig(
            url=URL,
            services=["VehiclePosition"],
            filter=FilterConfig(route_types=[2]),
        )
        service = self._service(api, [StaticConfig(url="https://example.org/gtfs.zip")])
        self._run(service)
        self.assertEqual(self._individual(), [])
        self.assertTrue(self._status()["skipped_no_static"])

    def test_route_filter_needs_static(self):
        with self.assertRaises(ValueError):
            ProviderConfig(
                name="p",
                realtime=[
                    ApiConfig(
                        url=URL,
                        services=["VehiclePosition"],
                        filter=FilterConfig(route_types=[2]),
                    )
                ],
            )

    def test_invalid_route_type_range(self):
        with self.assertRaises(ValueError):
            FilterConfig(route_types=["rail"])
        self.assertEqual(
            FilterConfig(route_types=[2, "100-102"]).route_type_set(),
            {2, 100, 101, 102},
        )

    def id_tmp(self):
        import tempfile

        directory = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, directory)
        return directory


class TestRetries(unittest.TestCase):
    def _response(self, status, content=b"ok"):
        response = MagicMock(status_code=status, content=content, reason="", url=URL)
        response.raise_for_status.side_effect = (
            requests.HTTPError(str(status)) if status >= 400 else None
        )
        return response

    def test_retries_then_succeeds(self):
        logger = MagicMock()
        with (
            patch(
                "src.gtfs_rt_aggregator.utils.http.requests.get",
                side_effect=[
                    requests.exceptions.SSLError("TLS dropped"),
                    self._response(503),
                    self._response(200, b"feed"),
                ],
            ) as get,
            patch("src.gtfs_rt_aggregator.utils.http.time.sleep") as sleep,
        ):
            self.assertEqual(get_bytes(URL, None, 3, logger), b"feed")
        self.assertEqual(get.call_count, 3)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [1.0, 2.0])

    def test_gives_up(self):
        with (
            patch(
                "src.gtfs_rt_aggregator.utils.http.requests.get",
                side_effect=requests.ConnectionError("down"),
            ) as get,
            patch("src.gtfs_rt_aggregator.utils.http.time.sleep"),
        ):
            with self.assertRaises(requests.ConnectionError):
                get_bytes(URL, None, 2, MagicMock())
        self.assertEqual(get.call_count, 3)

    def test_not_found_not_retried(self):
        with patch(
            "src.gtfs_rt_aggregator.utils.http.requests.get",
            return_value=self._response(404),
        ) as get:
            with self.assertRaises(requests.HTTPError):
                get_bytes(URL, None, 3, MagicMock())
        self.assertEqual(get.call_count, 1)


if __name__ == "__main__":
    unittest.main()


class TestKeepUnmatchedAdded(unittest.TestCase):
    def _entity(self, relationship, route_id, trip_id="X1"):
        from google.transit import gtfs_realtime_pb2

        entity = gtfs_realtime_pb2.FeedEntity(id="e")
        trip = entity.trip_update.trip
        trip.trip_id = trip_id
        if route_id:
            trip.route_id = route_id
        trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Value(relationship)
        )
        return entity

    def test_rules(self):
        from src.gtfs_rt_aggregator.fetcher.filter import EntityFilter

        entity_filter = EntityFilter(
            {2},
            {"rail"},
            {"T-rail"},
            known_routes={"rail", "bus"},
            keep_unmatched_added=True,
            known_trips={"T-rail", "T-bus"},
        )
        cases = [
            ("ADDED", "", True),  # route unknown: kept
            ("NEW", "unknown-route", True),  # route not in the static feed: kept
            ("ADDED", "bus", False),  # resolved to an unwanted route
            ("ADDED", "rail", True),  # resolved to a wanted route
            ("DUPLICATED", "", True),
            ("SCHEDULED", "", False),  # only added trips
        ]
        for relationship, route, expected in cases:
            with self.subTest(relationship=relationship, route=route):
                self.assertEqual(
                    entity_filter.keep(self._entity(relationship, route)), expected
                )

        # A duplicated trip points at a static trip: a bus trip is dropped
        self.assertFalse(entity_filter.keep(self._entity("DUPLICATED", "", "T-bus")))

        without = EntityFilter({2}, {"rail"}, set(), known_routes={"rail"})
        self.assertFalse(without.keep(self._entity("ADDED", "")))

    def test_unknown_trips_counted(self):
        from src.gtfs_rt_aggregator.fetcher.filter import EntityFilter

        entity_filter = EntityFilter({2}, {"rail"}, {"T-rail"}, known_routes={"rail", "bus"})
        cases = [
            ("bus", "T-bus", False),  # a known route of another mode
            ("", "T-new", True),  # a trip the static version does not have
            ("new-route", "T-new", True),
            ("", "", False),  # nothing to match
        ]
        for route, trip, expected in cases:
            with self.subTest(route=route, trip=trip):
                entity = self._entity("SCHEDULED", route, trip)
                self.assertFalse(entity_filter.keep(entity))
                self.assertEqual(entity_filter.unknown(entity), expected)

    def test_other_modes(self):
        import pyarrow as pa

        from src.gtfs_rt_aggregator.fetcher.filter import EntityFilter, trip_keys
        from google.transit import gtfs_realtime_pb2

        entity_filter = EntityFilter(
            {2},
            {"rail"},
            {"T-rail"},
            known_routes={"rail"},
            other_routes={"bus"},
            other_trips=trip_keys(pa.chunked_array([["T-bus", "T-tram", None]])),
        )
        cases = [
            # relationship, route, trip, kept, unknown
            ("SCHEDULED", "", "T-rail", True, False),
            ("SCHEDULED", "rail", "T-new", True, False),  # a new trip of a rail route
            ("SCHEDULED", "", "T-bus", False, False),  # a bus trip, by its id only
            ("SCHEDULED", "bus", "", False, False),  # a bus route, no trip id
            ("SCHEDULED", "", "T-new", True, True),  # unknown to the static feed: kept
            ("SCHEDULED", "new-route", "", True, True),
            ("SCHEDULED", "new-route", "T-tram", False, False),  # the trip tells the mode
            ("ADDED", "", "T-new", True, True),
            ("ADDED", "bus", "T-new", False, False),
            ("DUPLICATED", "", "T-bus", False, False),  # a copy of a bus trip
            ("DUPLICATED", "", "T-rail", True, False),
            ("SCHEDULED", "", "", False, False),  # nothing to match
        ]
        for relationship, route, trip, kept, unknown in cases:
            with self.subTest(relationship=relationship, route=route, trip=trip):
                entity = self._entity(relationship, route, trip)
                self.assertEqual(entity_filter.keep(entity), kept)
                self.assertEqual(entity_filter.unknown(entity), unknown)

        # Vehicle positions: matched the same way, never without a trip
        vehicle = gtfs_realtime_pb2.FeedEntity(id="v")
        vehicle.vehicle.vehicle.id = "1"
        self.assertFalse(entity_filter.keep(vehicle))
        vehicle.vehicle.trip.trip_id = "T-new"
        self.assertTrue(entity_filter.keep(vehicle))
        vehicle.vehicle.trip.trip_id = "T-bus"
        self.assertFalse(entity_filter.keep(vehicle))

    def test_needs_a_route_filter(self):
        with self.assertRaises(ValueError):
            FilterConfig(trip_ids=["T1"], keep_unmatched_added=True)


class TestFilterResolution(unittest.TestCase):
    """build_filter on small static versions: which trips count as another mode."""

    def setUp(self):
        import tempfile

        from src.gtfs_rt_aggregator.fetcher import filter as filter_module

        self.filter_module = filter_module
        self.storage = MockStorageInterface()
        tmp = tempfile.mkdtemp()
        self.addCleanup(__import__("shutil").rmtree, tmp)
        patcher = patch("tempfile.gettempdir", return_value=tmp)
        patcher.start()
        self.addCleanup(patcher.stop)
        filter_module._MEMO.clear()
        self.addCleanup(filter_module._MEMO.clear)

    def test_unfiltered_version_of_the_wanted_mode_only(self):
        # A rail operator's timetable next to an all-operator realtime feed: a
        # bus missing from it is not an unknown train
        tables = self._tables({"RAIL": 2}, {"T1": "RAIL"})
        entity_filter = self._filter(FilterConfig(route_types=[2]), tables)
        self.assertFalse(entity_filter.knows_other_modes)
        self.assertTrue(entity_filter.keep(self._entity("T1")))
        self.assertFalse(entity_filter.keep(self._entity("B-missing")))
        self.assertTrue(entity_filter.unknown(self._entity("B-missing")))

    def test_null_route_ids(self):
        # A broken feed's null route id matches nothing, and does not fail
        tables = self._tables({"RAIL": 2, None: 3}, {"T1": "RAIL", "X": None}, [("B1", "BUS", 3)])
        entity_filter = self._filter(FilterConfig(route_types=[2]), tables)
        self.assertTrue(entity_filter.keep(self._entity("T1")))
        self.assertTrue(entity_filter.knows_other_modes)
        self.assertEqual(entity_filter.other_routes, {"BUS"})

    def test_broken_disk_cache_resolved_again(self):
        side = [("B1", "BUS", 3)]
        tables = self._tables({"RAIL": 2}, {"T1": "RAIL"}, side)
        config = FilterConfig(route_types=[2])
        self._filter(config, tables)
        import glob
        import tempfile

        (keys,) = glob.glob(os.path.join(tempfile.gettempdir(), "gtfs_rt_aggregator-*", "*.npz"))
        with open(keys, "wb") as f:
            f.write(b"cut")
        self.filter_module._MEMO.clear()
        entity_filter = self._filter(config, tables)
        self.assertFalse(entity_filter.keep(self._entity("B1")))
        # Written again
        self.filter_module._MEMO.clear()
        with patch.object(self.filter_module, "_resolve", side_effect=AssertionError):
            self.assertFalse(self._filter(config, tables).keep(self._entity("B1")))

    def test_added_trips_kept_by_keep_unmatched_added_not_counted_kept_unknown(self):
        from src.gtfs_rt_aggregator.fetcher.filter import EntityFilter

        entity_filter = EntityFilter(
            {2}, {"rail"}, {"T-rail"}, known_routes={"rail"}, keep_unmatched_added=True
        )
        # Kept and unknown, but the runtime counts kept_unknown only for versions telling the other modes
        self.assertEqual(entity_filter.classify(self._entity("T-new", relationship="ADDED")), (True, True))
        self.assertFalse(entity_filter.knows_other_modes)

    def _tables(self, routes, trips, side=None):
        """routes: {route_id: type}; trips: {trip_id: route_id}; side: rows of the side table."""
        tables = {"routes": "v/routes.parquet", "trips": "v/trips.parquet"}
        self.storage.save_bytes(
            _parquet(
                pa.table(
                    {
                        "route_id": list(routes),
                        "route_type": pa.array(list(routes.values()), pa.int16()),
                    }
                )
            ),
            tables["routes"],
        )
        self.storage.save_bytes(
            _parquet(pa.table({"trip_id": list(trips), "route_id": list(trips.values())})),
            tables["trips"],
        )
        if side is not None:
            tables[self.filter_module.OTHER_MODES] = "v/_other_modes.parquet"
            self.storage.save_bytes(
                _parquet(
                    pa.table(
                        {
                            "trip_id": [t for t, _, _ in side],
                            "route_id": [r for _, r, _ in side],
                            "route_type": pa.array([k for _, _, k in side], pa.int32()),
                        }
                    )
                ),
                tables[self.filter_module.OTHER_MODES],
            )
        return tables

    def _filter(self, config, tables):
        return self.filter_module.build_filter(config, self.storage, tables, "k")

    @staticmethod
    def _entity(trip_id="", route_id="", relationship="SCHEDULED"):
        from google.transit import gtfs_realtime_pb2

        entity = gtfs_realtime_pb2.FeedEntity(id="e")
        trip = entity.trip_update.trip
        if trip_id:
            trip.trip_id = trip_id
        if route_id:
            trip.route_id = route_id
        trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Value(relationship)
        )
        return entity

    def test_route_ids_filter_drops_unknown_trips(self):
        # A route_ids filter asks for its routes only, also on an unfiltered version
        tables = self._tables({"R1": 3, "R2": 3}, {"T1": "R1", "T2": "R2"})
        entity_filter = self._filter(FilterConfig(route_ids=["R1"]), tables)
        self.assertFalse(entity_filter.knows_other_modes)
        self.assertTrue(entity_filter.keep(self._entity("T1")))
        self.assertFalse(entity_filter.keep(self._entity("T2")))
        self.assertFalse(entity_filter.keep(self._entity("T-new")))
        self.assertFalse(entity_filter.keep(self._entity("T-new", relationship="ADDED")))
        with_added = self._filter(
            FilterConfig(route_ids=["R1"], keep_unmatched_added=True), tables
        )
        self.assertTrue(with_added.keep(self._entity("T-new", relationship="ADDED")))

    def test_unfiltered_version_drops_unknown_trips_whatever_its_routes(self):
        # A rail operator's timetable with a few replacement buses, next to a
        # realtime feed of every operator: the other operators' buses are unknown
        tables = self._tables({"RAIL": 2, "BUS": 3}, {"T1": "RAIL", "B1": "BUS"})
        entity_filter = self._filter(FilterConfig(route_types=[2]), tables)
        self.assertFalse(entity_filter.knows_other_modes)
        self.assertTrue(entity_filter.keep(self._entity("T1")))
        self.assertFalse(entity_filter.keep(self._entity("B1")))
        self.assertFalse(entity_filter.keep(self._entity("B-other-operator")))
        self.assertTrue(entity_filter.unknown(self._entity("B-other-operator")))
        # keep_unmatched_added applies
        with_added = self._filter(
            FilterConfig(route_types=[2], keep_unmatched_added=True), tables
        )
        self.assertTrue(with_added.keep(self._entity("T-new", relationship="ADDED")))

    def test_alerts_follow_the_trip_rule(self):
        from google.transit import gtfs_realtime_pb2

        tables = self._tables({"RAIL": 2}, {"T1": "RAIL"}, [("B1", "BUS", 3)])
        entity_filter = self._filter(FilterConfig(route_types=[2]), tables)

        def alert(**fields):
            entity = gtfs_realtime_pb2.FeedEntity(id="a")
            selector = entity.alert.informed_entity.add()
            for name, value in fields.items():
                if name.startswith("trip_"):
                    setattr(selector.trip, name[5:], value)
                else:
                    setattr(selector, name, value)
            return entity

        cases = [
            ({"trip_trip_id": "T1"}, True),
            ({"trip_trip_id": "T-new"}, True),  # unknown: kept, as trips are
            ({"route_id": "R-new"}, True),
            ({"trip_trip_id": "B1"}, False),  # another mode
            ({"route_id": "BUS"}, False),
            ({"trip_route_id": "BUS", "trip_trip_id": "T-new"}, False),
            ({"stop_id": "S1"}, False),  # nothing to match
            ({"route_type": 3}, False),  # another route type
            ({"route_type": 2}, True),
        ]
        for fields, expected in cases:
            with self.subTest(**fields):
                self.assertEqual(entity_filter.keep(alert(**fields)), expected)
        # Without the side table, as before
        old = self._filter(FilterConfig(route_types=[2]), self._tables({"RAIL": 2}, {"T1": "RAIL"}))
        self.assertFalse(old.keep(alert(trip_trip_id="T-new")))

    def test_cache_written_by_many_threads(self):
        import glob
        import tempfile
        import threading

        tables = self._tables({"RAIL": 2}, {"T1": "RAIL"}, [("B1", "BUS", 3)])
        config = FilterConfig(route_types=[2])
        errors, filters = [], []

        def build():
            try:
                filters.append(self.filter_module.build_filter(config, self.storage, tables, "k"))
            except Exception as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=build) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        folder = glob.glob(os.path.join(tempfile.gettempdir(), "gtfs_rt_aggregator-*"))[0]
        self.assertEqual([f for f in os.listdir(folder) if f.endswith(".tmp")], [])
        self.filter_module._MEMO.clear()
        with patch.object(self.filter_module, "_resolve", side_effect=AssertionError):
            self.assertFalse(self._filter(config, tables).keep(self._entity("B1")))

    def test_trips_of_a_missing_route_are_unknown(self):
        # Their route is in no routes table: unknown in a filtered version (inner join)
        # and in an unfiltered one alike
        tables = self._tables({"RAIL": 2, "BUS": 3}, {"T1": "RAIL", "X1": "GONE"}, [])
        entity_filter = self._filter(FilterConfig(route_types=[2]), tables)
        self.assertTrue(entity_filter.keep(self._entity("X1")))
        self.assertTrue(entity_filter.unknown(self._entity("X1")))

    def test_side_rows_the_filter_asks_for_are_kept(self):
        side = [("B1", "BUS1", 3), ("B2", "BUS2", 3), (None, "TRAM", 0), ("F1", "FERRY", 4)]
        tables = self._tables({"RAIL": 2}, {"T1": "RAIL"}, side)
        # A bus route asked by route_ids, matched by a trip id only
        by_route = self._filter(FilterConfig(route_types=[2], route_ids=["BUS1"]), tables)
        self.assertTrue(by_route.keep(self._entity("B1")))
        self.assertFalse(by_route.unknown(self._entity("B1")))
        self.assertFalse(by_route.keep(self._entity("B2")))
        self.assertFalse(by_route.keep(self._entity("F1")))
        # Alerts naming a side-table trip by its id only
        from google.transit import gtfs_realtime_pb2

        for trip_id, expected in (("B1", True), ("F1", False)):
            alert = gtfs_realtime_pb2.FeedEntity(id="a")
            alert.alert.informed_entity.add().trip.trip_id = trip_id
            self.assertEqual(by_route.keep(alert), expected)
        # Route types wider than the static ones
        by_type = self._filter(FilterConfig(route_types=[2, 3]), tables)
        for trip_id in ("B1", "B2"):
            self.assertTrue(by_type.keep(self._entity(trip_id)))
        self.assertTrue(by_type.keep(self._entity(route_id="BUS2")))
        self.assertFalse(by_type.keep(self._entity("F1")))
        self.assertFalse(by_type.keep(self._entity(route_id="TRAM")))
        self.assertTrue(by_type.keep(self._entity("T-new")))
        # The same from the disk cache of another worker
        self.filter_module._MEMO.clear()
        with patch.object(self.filter_module, "_resolve", side_effect=AssertionError):
            cached = self._filter(FilterConfig(route_types=[2, 3]), tables)
        self.assertTrue(cached.keep(self._entity("B1")))
        self.assertFalse(cached.keep(self._entity("F1")))

    def test_kept_route_ids_not_counted_unknown(self):
        from src.gtfs_rt_aggregator.fetcher.filter import EntityFilter

        # A version without the side table: a kept route_ids route is not unknown
        entity_filter = EntityFilter({2}, {"rail", "asked"}, set(), known_routes={"rail"})
        entity = self._entity("T-x", "asked")
        self.assertTrue(entity_filter.keep(entity))
        self.assertFalse(entity_filter.unknown(entity))


# Integers MessageToDict writes as strings (64-bit)
_INT64_KEYS = {"timestamp", "time", "start", "end", "lastModifiedTime"}


def _ints(value):
    if isinstance(value, dict):
        return {
            k: int(v) if k in _INT64_KEYS and isinstance(v, str) else _ints(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [_ints(v) for v in value]
    return value


def _dict_table(entities, service, fetch_time, meta):
    """Reference: the table built through MessageToDict (the path before 0.6.0)."""
    from datetime import timezone

    from google.protobuf.json_format import MessageToDict

    from src.gtfs_rt_aggregator.fetcher.gtfs_rt import SERVICE_TYPE_TO_SCHEMA

    key = {"TripUpdate": "tripUpdate", "VehiclePosition": "vehicle", "Alert": "alert"}[
        service
    ]
    rows = []
    for entity in entities:
        data = MessageToDict(entity)
        if key in data:
            rows.append(
                {
                    "entityId": data.get("id"),
                    "contentHash": GtfsRtFetcher.entity_hash(entity),
                    **_ints(data[key]),
                    "fetchTime": fetch_time.astimezone(timezone.utc),
                    **meta,
                }
            )
    table = pa.Table.from_pylist(rows, schema=SERVICE_TYPE_TO_SCHEMA[service]).flatten()
    return table.rename_columns([c.replace(".", "_") for c in table.column_names])


class TestArrowBuilder(unittest.TestCase):
    """The direct protobuf -> Arrow builder gives the same tables as MessageToDict."""

    def test_same_as_dict_path(self):
        from datetime import datetime

        import pytz

        from src.gtfs_rt_aggregator.fetcher.gtfs_rt import row_metadata

        fetch_time = datetime(2026, 9, 29, 10, tzinfo=pytz.UTC)
        for name, service in (
            ("trip_updates", "TripUpdate"),
            ("vehicle_positions", "VehiclePosition"),
            ("alerts", "Alert"),
        ):
            with self.subTest(service):
                with open(os.path.join(DATA, f"{name}.pb"), "rb") as f:
                    message = GtfsRtFetcher.parse_message(f.read())
                entities = list(message.entity)
                meta = row_metadata("p", fetch_time, message.header.timestamp, "v1")
                expected = _dict_table(entities, service, fetch_time, meta)
                actual = GtfsRtFetcher.build_tables(
                    entities,
                    [GtfsRtFetcher.entity_hash(e) for e in entities],
                    [service],
                    fetch_time,
                    meta,
                )[service]
                self.assertTrue(actual.equals(expected))

    def test_trip_modifications(self):
        from datetime import datetime

        from google.transit import gtfs_realtime_pb2

        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        entity = message.entity.add(id="m1")
        entity.trip_modifications.selected_trips.add(trip_ids=["T1"], shape_id="S1")
        entity.trip_modifications.modifications.add(last_modified_time=1742550861)
        entities = list(message.entity)
        table = GtfsRtFetcher.build_tables(
            entities, ["h"], ["TripModifications"], datetime.now()
        )["TripModifications"]
        row = table.to_pylist()[0]
        self.assertEqual(row["selectedTrips"], [{"tripIds": ["T1"], "shapeId": "S1"}])
        self.assertEqual(row["modifications"][0]["lastModifiedTime"], 1742550861)


class TestOddValues(unittest.TestCase):
    def test_severity_and_huge_timestamps(self):
        from datetime import datetime

        import pytz
        from google.transit import gtfs_realtime_pb2

        from src.gtfs_rt_aggregator.fetcher.gtfs_rt import row_metadata

        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        message.header.timestamp = 2**63 + 5
        alert = message.entity.add(id="a").alert
        alert.severity_level = gtfs_realtime_pb2.Alert.SeverityLevel.Value("WARNING")
        vehicle = message.entity.add(id="v").vehicle
        vehicle.timestamp = 2**63 + 5
        vehicle.trip.trip_id = "T"
        entities = list(message.entity)
        now = datetime.now(pytz.UTC)
        meta = row_metadata("p", now, message.header.timestamp, None)
        self.assertIsNone(meta["feedTimestamp"])
        tables = GtfsRtFetcher.build_tables(
            entities,
            [GtfsRtFetcher.entity_hash(e) for e in entities],
            ["Alert", "VehiclePosition"],
            now,
            meta,
        )
        self.assertEqual(tables["Alert"]["severityLevel"].to_pylist(), ["WARNING"])
        self.assertEqual(tables["VehiclePosition"]["timestamp"].to_pylist(), [None])

    def test_conform_huge_legacy_values(self):
        from src.gtfs_rt_aggregator.schema.conform import conform

        legacy = pa.table(
            {
                "entityId": ["a", "b"],
                "fetchTime": pa.array([1742550861, 2**63 + 5], pa.uint64()),
                "timestamp": pa.array([1, 2**63 + 5], pa.uint64()),
            }
        )
        table = conform(legacy, "VehiclePosition", "p")
        self.assertEqual(table["fetchTime"].null_count, 1)
        self.assertEqual(table.schema.field("timestamp").type, pa.int64())
