"""Iceberg sink on filesystem storage."""

import json
import os
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    IcebergConfig,
    OutputConfig,
    ProviderConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.fetcher.gtfs_rt import GtfsRtFetcher, row_metadata
from src.gtfs_rt_aggregator.sinks.iceberg import IcebergSink
from src.gtfs_rt_aggregator.storage.filesystem import FileSystemStorage

DATA = os.path.join(os.path.dirname(__file__), "..", "data")


class TestIcebergSink(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = FileSystemStorage(os.path.join(self.tmp, "out"))
        self.config = GtfsRtConfig(
            storage=StorageConfig(
                type="filesystem",
                params={"base_directory": os.path.join(self.tmp, "out")},
            ),
            providers=[
                ProviderConfig(
                    name=name,
                    timezone="Europe/Brussels",
                    realtime=[
                        ApiConfig(
                            url=f"https://x.org/{name}", services=["VehiclePosition"]
                        )
                    ],
                )
                for name in ("be", "nl")
            ],
            output=OutputConfig(compact_daily=True),
            iceberg=IcebergConfig(
                catalog_uri=f"sqlite:///{self.tmp}/catalog.db",
                services=["VehiclePosition"],
            ),
        )
        self.sink = IcebergSink(self.config, {"global": self.storage})
        with open(os.path.join(DATA, "vehicle_positions.pb"), "rb") as f:
            self.message = GtfsRtFetcher.parse_message(f.read())

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def _day(self, provider, days_ago, entities=None, name="day.parquet"):
        """Write a compacted day file like the aggregator does."""
        tz = pytz.timezone("Europe/Brussels")
        day = datetime.now(tz).date() - timedelta(days=days_ago)
        fetch_time = tz.localize(datetime(day.year, day.month, day.day, 12))
        entities = list(self.message.entity)[:entities]
        table = GtfsRtFetcher.build_tables(
            entities,
            [GtfsRtFetcher.entity_hash(e) for e in entities],
            ["VehiclePosition"],
            fetch_time,
            row_metadata(provider, fetch_time, self.message.header.timestamp, None),
        )["VehiclePosition"]
        start = tz.localize(datetime(day.year, day.month, day.day))
        folder = os.path.dirname(
            self.config.output.path_template.format(
                provider=provider, service="VehiclePosition", start=start, end=start
            )
        )
        path = f"{folder}/{name}"
        os.makedirs(os.path.join(self.tmp, "out", folder), exist_ok=True)
        pq.write_table(table, os.path.join(self.tmp, "out", path))
        return table.num_rows

    def _rows(self):
        table = self.sink.catalog.load_table("archive.VehiclePosition")
        return table.scan().to_arrow()

    def test_register_and_replace(self):
        expected = self._day("be", 1) + self._day("nl", 1) + self._day("nl", 2)
        self.assertEqual(self.sink.sync(), 3)
        self.assertEqual(self._rows().num_rows, expected)
        # Nothing new
        self.assertEqual(self.sink.sync(), 0)

        # The day compacted again (late files): replaces its earlier file
        before = self._day("nl", 1)
        after = self._day("nl", 1, entities=100)
        self.assertEqual(self.sink.sync(), 1)
        self.assertEqual(self._rows().num_rows, expected - before + after)

        # Partitioned by provider and date
        table = self.sink.catalog.load_table("archive.VehiclePosition")
        self.assertEqual([f.name for f in table.spec().fields], ["provider", "date"])

        hint = os.path.join(
            self.tmp,
            "out",
            "iceberg",
            "VehiclePosition",
            "metadata",
            "version-hint.text",
        )
        with open(hint) as f:
            self.assertTrue(
                table.metadata_location.endswith(f.read() + ".metadata.json")
            )
        self.sink.maintain()

    def test_read_without_catalog(self):
        expected = self._day("be", 1) + self._day("nl", 1)
        self.sink.sync()
        location = os.path.join(self.tmp, "out", "iceberg", "VehiclePosition")
        try:
            import duckdb

            connection = duckdb.connect()
            connection.sql("INSTALL iceberg; LOAD iceberg")
        except Exception:
            self.skipTest("DuckDB or its iceberg extension not available")
        count = connection.sql(
            f"SELECT count(*) FROM iceberg_scan('{location}')"
        ).fetchone()[0]
        self.assertEqual(count, expected)
        try:
            import polars as pl
        except ImportError:
            return
        table = self.sink.catalog.load_table("archive.VehiclePosition")
        self.assertEqual(
            pl.scan_iceberg(table.metadata_location).select(pl.len()).collect().item(),
            expected,
        )

    def _serve(self):
        """Serve the storage folder over http, like a public host would."""
        import functools
        import http.server
        import threading

        class Quiet(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *args):
                pass

        handler = functools.partial(Quiet, directory=os.path.join(self.tmp, "out"))
        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_address[1]}"

    def _public_strings(self):
        """Every string of the public metadata files."""
        import fastavro

        folder = os.path.join(
            self.tmp, "out", "iceberg-public", "VehiclePosition", "metadata"
        )
        strings = []

        def walk(value):
            if isinstance(value, str):
                strings.append(value)
            elif isinstance(value, dict):
                [walk(v) for v in value.values()]
            elif isinstance(value, list):
                [walk(v) for v in value]

        for name in os.listdir(folder):
            with open(os.path.join(folder, name), "rb") as f:
                if name.endswith(".avro"):
                    [walk(record) for record in fastavro.reader(f)]
                elif name.endswith(".json"):
                    walk(json.load(f))
        return os.listdir(folder), strings

    def test_public_copy(self):
        base = self._serve()
        self.sink.settings.public_base_url = base
        self.sink.settings.public_warehouse = "iceberg-public"
        be = self._day("be", 1)
        self._day("nl", 1)
        self.sink.sync()
        # Compacted again: a new version, the old public files go
        expected = be + self._day("nl", 1, entities=100)
        self.sink.sync()
        names, strings = self._public_strings()
        self.assertEqual(len([n for n in names if n.endswith(".metadata.json")]), 1)
        self.assertIn("version-hint.text", names)
        self.assertFalse([s for s in strings if s.startswith("file:")])
        paths = [s for s in strings if s.startswith(base + "/")]
        self.assertTrue(paths)
        for path in paths:
            if path.endswith((".parquet", ".avro", ".json")):
                self.assertTrue(
                    os.path.exists(
                        os.path.join(self.tmp, "out", path[len(base) + 1 :])
                    ),
                    path,
                )
        # Nothing new: nothing written
        self.assertFalse(
            self.sink._publish(
                "VehiclePosition", self.sink._existing_table("VehiclePosition")
            )
        )
        try:
            import duckdb

            connection = duckdb.connect()
            connection.sql("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs")
        except Exception:
            self.skipTest("DuckDB or its extensions not available")
        url = f"{base}/iceberg-public/VehiclePosition"
        count = connection.sql(
            f"SELECT count(*) FROM iceberg_scan('{url}')"
        ).fetchone()[0]
        self.assertEqual(count, expected)
        count = connection.sql(
            f"SELECT count(*) FROM iceberg_scan('{url}') WHERE provider = 'be'"
        ).fetchone()[0]
        self.assertEqual(count, be)

    def test_concurrent_registration(self):
        from src.gtfs_rt_aggregator.sinks.iceberg import IcebergSink

        self._day("be", 2)
        self.sink.sync()
        stale = self.sink._existing_table("VehiclePosition")
        self._day("be", 1)
        # Another process (e.g. --iceberg-backfill) registers the day first
        other = IcebergSink(self.config, {"global": self.sink.storage})
        self.assertEqual(other.sync(), 1)
        path = self.sink._day_files(
            self.config.providers[0], "VehiclePosition", "day.parquet", 1
        )[0]
        _, registered = self.sink._register(
            "VehiclePosition", "be", path, self.sink.storage.uri(path), stale
        )
        self.assertFalse(registered)

    def test_backfill_compacts_old_days(self):
        from src.gtfs_rt_aggregator.aggregator.convert import compact_old_days

        old = self._day("be", 20, name="08-00-00_to_09-00-00.parquet")
        old += self._day("be", 20, name="09-00-00_to_10-00-00.parquet")
        self._day("be", 3, name="08-00-00_to_09-00-00.parquet")
        compact_old_days(self.config, {"global": self.sink.storage})
        out = os.path.join(self.tmp, "out")
        found = sorted(
            os.path.relpath(os.path.join(d, f), out)
            for d, _, files in os.walk(out)
            for f in files
            if f.endswith(".parquet")
        )
        # The old day is compacted, the recent one left to the pipeline
        self.assertEqual(
            [f.rsplit("/", 1)[1] for f in found],
            ["day.parquet", "08-00-00_to_09-00-00.parquet"],
        )
        self.assertEqual(pq.read_table(os.path.join(out, found[0])).num_rows, old)
        self.assertEqual(self.sink.sync(days_back=None), 1)

    def test_day_without_rows_skipped(self):
        # e.g. no alert all day: nothing to register, the other days are
        self.assertEqual(self._day("be", 1, entities=0), 0)
        expected = self._day("nl", 1)
        self.assertEqual(self.sink.sync(), 1)
        self.assertEqual(self._rows().num_rows, expected)

    def test_old_file_skipped(self):
        path = "provider=be/service=VehiclePosition/date=2000-01-01/day.parquet"
        os.makedirs(os.path.dirname(os.path.join(self.tmp, "out", path)))
        pq.write_table(
            pa.table({"entityId": ["a"], "fetchTime": pa.array([1], pa.uint64())}),
            os.path.join(self.tmp, "out", path),
        )
        with self.assertLogs(level="WARNING"):
            self.assertEqual(self.sink.sync(days_back=None), 0)


class TestIcebergConfig(unittest.TestCase):
    def _config(self, **output):
        return GtfsRtConfig(
            storage=StorageConfig(type="filesystem"),
            providers=[
                ProviderConfig(
                    name="p", realtime=[ApiConfig(url="u", services=["Alert"])]
                )
            ],
            output=OutputConfig(**output),
            iceberg=IcebergConfig(catalog_uri="sqlite:///x.db"),
        )

    def test_needs_compaction(self):
        with self.assertRaisesRegex(ValueError, "compact_daily"):
            self._config()

    def test_missing_extra(self):
        with patch("importlib.util.find_spec", return_value=None):
            with self.assertRaisesRegex(ValueError, r"\[iceberg\]"):
                self._config(compact_daily=True)


if __name__ == "__main__":
    unittest.main()
