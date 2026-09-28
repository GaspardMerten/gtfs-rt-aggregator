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
        self.addCleanup(service.flush_all)
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
        self.assertEqual(set(table["feedTimestamp"].to_pylist()), {1742550861})
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

    def test_unchanged_fetch_writes_ended_window(self):
        from datetime import datetime

        import pytz

        api = ApiConfig(url=URL, services=["VehiclePosition"], accumulate_minutes=15)
        service = self._service(api)

        class _Now(datetime):
            current = datetime(2026, 1, 1, 16, 1, tzinfo=pytz.UTC)

            @classmethod
            def now(cls, tz=None):
                return cls.current.astimezone(tz)

        with patch("src.gtfs_rt_aggregator.fetcher.service.datetime", _Now):
            self._run(service)
            self.assertEqual(self._individual(), [])
            # Next window, same content: not stored, but the ended window is written
            _Now.current = datetime(2026, 1, 1, 16, 20, tzinfo=pytz.UTC)
            self._run(service)
        self.assertEqual(len(self._individual()), 1)


class TestTripModifications(unittest.TestCase):
    def test_fields_kept(self):
        from datetime import datetime

        from google.transit import gtfs_realtime_pb2

        message = gtfs_realtime_pb2.FeedMessage()
        message.header.gtfs_realtime_version = "2.0"
        entity = message.entity.add(id="m1")
        selected = entity.trip_modifications.selected_trips.add()
        selected.trip_ids.append("T1")
        selected.shape_id = "S1"
        modification = entity.trip_modifications.modifications.add()
        modification.last_modified_time = 1742550861
        modification.start_stop_selector.stop_id = "A"

        header, parsed = GtfsRtFetcher.parse_feed_with_header(
            message.SerializeToString()
        )
        table = GtfsRtFetcher.to_tables(parsed, ["TripModifications"], datetime.now())[
            "TripModifications"
        ]
        row = table.to_pylist()[0]
        self.assertEqual(row["selectedTrips"], [{"tripIds": ["T1"], "shapeId": "S1"}])
        self.assertEqual(row["modifications"][0]["lastModifiedTime"], 1742550861)


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
        self.assertEqual(self._status()["kept_count"], rail)

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

    def test_no_static_version_yet_keeps_everything(self):
        api = ApiConfig(
            url=URL,
            services=["VehiclePosition"],
            filter=FilterConfig(route_types=[2]),
        )
        service = self._service(api, [StaticConfig(url="https://example.org/gtfs.zip")])
        self._run(service)
        self.assertEqual(self._rows().num_rows, 3549)

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
