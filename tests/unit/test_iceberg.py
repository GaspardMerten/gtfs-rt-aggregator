"""Iceberg sink on filesystem storage."""

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

    def _day(self, provider, days_ago, entities=None):
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
        path = f"{folder}/day.parquet"
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
