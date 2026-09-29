"""Environment variables, output paths, deduplication, compaction, static URLs."""

import io
import json
import unittest
from datetime import datetime, timedelta
from io import BytesIO
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from src.gtfs_rt_aggregator.aggregator.dedup import deduplicate
from src.gtfs_rt_aggregator.aggregator.service import AggregatorService
from src.gtfs_rt_aggregator.config.loader import expand_env, load_config_from_toml_file
from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    OutputConfig,
    ProviderConfig,
    StaticConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.static.service import StaticService
from tests.mocks import MockStorageInterface
from tests.unit.test_static_service import GTFS_FILES, _make_zip

STORAGE = '[storage]\ntype = "filesystem"\n'


def _load(toml: str):
    return load_config_from_toml_file(BytesIO(toml.encode()))


class TestEnvironment(unittest.TestCase):
    def test_expand(self):
        with patch.dict("os.environ", {"KEY": "s3cret"}):
            self.assertEqual(
                expand_env(
                    {"h": {"x-api-key": "${KEY}"}, "u": ["a?k=${KEY}&$${KEY}"], "n": 5}
                ),
                {"h": {"x-api-key": "s3cret"}, "u": ["a?k=s3cret&${KEY}"], "n": 5},
            )

    def test_missing_variable(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(ValueError, "NOPE"):
                expand_env({"headers": {"k": "${NOPE}"}})

    def test_in_config(self):
        toml = STORAGE + """
[[providers]]
name = "uk"
  [[providers.realtime]]
  url = "https://example.org/feed?key=${UK_KEY}"
  services = ["VehiclePosition"]
  [providers.realtime.headers]
  x-api-key = "${UK_KEY}"
"""
        with patch.dict("os.environ", {"UK_KEY": "abc"}):
            api = _load(toml).providers[0].realtime[0]
        self.assertEqual(api.url, "https://example.org/feed?key=abc")
        self.assertEqual(api.headers, {"x-api-key": "abc"})


class TestOutputPaths(unittest.TestCase):
    def _aggregator(self, output=None):
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem"),
            providers=[
                ProviderConfig(
                    name="nl",
                    realtime=[ApiConfig(url="u", services=["VehiclePosition"])],
                )
            ],
            output=output or OutputConfig(),
        )
        return AggregatorService(config, {"global": MockStorageInterface()})

    def test_default_is_hive(self):
        start = datetime(2026, 9, 28, 16)
        self.assertEqual(
            self._aggregator().output_path(
                "nl", "VehiclePosition", start, start + timedelta(hours=1)
            ),
            "provider=nl/service=VehiclePosition/date=2026-09-28/16-00-00_to_17-00-00.parquet",
        )

    def test_legacy_options(self):
        config = _load(
            STORAGE
            + '[output]\nfilename_format = "{group_time}-{next_period}.parquet"\ntime_format = "%H%M"\n'
            + '[[providers]]\nname = "nl"\n[[providers.realtime]]\nurl = "u"\nservices = ["Alert"]\n'
        )
        start = datetime(2026, 9, 28, 16)
        path = config.output.path_template.format(
            provider="nl", service="Alert", start=start, end=start + timedelta(hours=1)
        )
        self.assertEqual(path, "nl/Alert/2026-09-28/1600-1700.parquet")

    def test_invalid_templates(self):
        for template in (
            "{provider}/{service}.parquet",  # no {start}
            "{provider}/{start:%H}.csv",
            "{unknown}/{start:%H}.parquet",
        ):
            with self.subTest(template), self.assertRaises(ValueError):
                OutputConfig(path_template=template)
        with self.assertRaises(ValueError):
            OutputConfig(
                path_template="{provider}/{start:%H}.parquet", compact_daily=True
            )
        with self.assertRaises(ValueError):
            # One folder per hour: a day is not in one folder
            OutputConfig(
                path_template="{provider}/{start:%Y-%m-%d}/{start:%H}/{start:%M}.parquet",
                compact_daily=True,
            )


def _rows(entity_ids, hashes, times):
    return pa.table(
        {
            "entityId": entity_ids,
            "contentHash": hashes,
            "fetchTime": pa.array(times, pa.uint64()),
        }
    )


class TestDeduplicate(unittest.TestCase):
    def test_consecutive_runs(self):
        table = deduplicate(
            _rows(
                ["a", "a", "a", "a", "b", "b"],
                ["x", "x", "y", "x", "z", "z"],
                [10, 20, 30, 40, 10, 20],
            )
        )
        runs = sorted(
            zip(
                table["entityId"].to_pylist(),
                table["contentHash"].to_pylist(),
                table["firstSeen"].to_pylist(),
                table["lastSeen"].to_pylist(),
            )
        )
        # a goes back to x: that is a new run, not merged with the first one
        self.assertEqual(
            runs,
            [
                ("a", "x", 10, 20),
                ("a", "x", 40, 40),
                ("a", "y", 30, 30),
                ("b", "z", 10, 20),
            ],
        )

    def test_merge_with_deduplicated_file(self):
        first = deduplicate(_rows(["a", "a"], ["x", "x"], [10, 20]))
        merged = deduplicate(
            pa.concat_tables(
                [first, _rows(["a"], ["x"], [30])], promote_options="default"
            )
        )
        self.assertEqual(merged.num_rows, 1)
        self.assertEqual(merged["firstSeen"].to_pylist(), [10])
        self.assertEqual(merged["lastSeen"].to_pylist(), [30])

    def test_gap_ends_run(self):
        # a is missing from the fetch at 20 (only b is there), then comes back
        table = deduplicate(_rows(["a", "b", "a"], ["x", "z", "x"], [10, 20, 30]))
        runs = sorted(
            zip(
                table["entityId"].to_pylist(),
                table["firstSeen"].to_pylist(),
                table["lastSeen"].to_pylist(),
            )
        )
        self.assertEqual(runs, [("a", 10, 10), ("a", 30, 30), ("b", 20, 20)])

    def test_rows_without_hash_kept(self):
        table = deduplicate(_rows(["a", "a"], [None, None], [10, 20]))
        self.assertEqual(table.num_rows, 2)


class TestCompaction(unittest.TestCase):
    def test_compacts_finished_days(self):
        storage = MockStorageInterface()
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem"),
            providers=[
                ProviderConfig(
                    name="nl",
                    timezone="Europe/Amsterdam",
                    realtime=[ApiConfig(url="u", services=["VehiclePosition"])],
                )
            ],
            output=OutputConfig(compact_daily=True),
        )
        aggregator = AggregatorService(config, {"global": storage})
        tz = pytz.timezone("Europe/Amsterdam")
        yesterday = datetime.now(tz).date() - timedelta(days=1)
        today = datetime.now(tz).date()
        for day, hours in ((yesterday, (10, 9)), (today, (8,))):
            for hour in hours:
                start = tz.localize(datetime(day.year, day.month, day.day, hour))
                path = aggregator.output_path(
                    "nl", "VehiclePosition", start, start + timedelta(hours=1)
                )
                buffer = io.BytesIO()
                pq.write_table(
                    _rows([f"e{hour}", "a"], ["h", "h"], [hour, hour]), buffer
                )
                storage.save_bytes(buffer.getvalue(), path)

        aggregator.compact_once("nl", ["VehiclePosition"], "Europe/Amsterdam")

        folder = f"provider=nl/service=VehiclePosition/date={yesterday:%Y-%m-%d}"
        self.assertEqual(storage.list_paths(folder), [f"{folder}/day.parquet"])
        day = pq.read_table(io.BytesIO(storage.get_bytes(f"{folder}/day.parquet")))
        # Sorted by entityId, then fetchTime
        self.assertEqual(day["entityId"].to_pylist(), ["a", "a", "e10", "e9"])
        # Written as Unix seconds (before 0.6.0): read back as timestamps
        self.assertEqual(
            [t.timestamp() for t in day["fetchTime"].to_pylist()], [9, 10, 10, 9]
        )
        self.assertEqual(set(day["provider"].to_pylist()), {"nl"})
        # Today is not over: left alone
        self.assertEqual(
            len(
                storage.list_paths(
                    f"provider=nl/service=VehiclePosition/date={today:%Y-%m-%d}"
                )
            ),
            1,
        )

        # A late file for yesterday is merged into the compacted day
        start = tz.localize(
            datetime(yesterday.year, yesterday.month, yesterday.day, 23)
        )
        buffer = io.BytesIO()
        pq.write_table(_rows(["z"], ["h"], [23]), buffer)
        storage.save_bytes(
            buffer.getvalue(),
            aggregator.output_path("nl", "VehiclePosition", start, start),
        )
        aggregator.compact_once("nl", ["VehiclePosition"], "Europe/Amsterdam")
        self.assertEqual(storage.list_paths(folder), [f"{folder}/day.parquet"])
        day = pq.read_table(io.BytesIO(storage.get_bytes(f"{folder}/day.parquet")))
        self.assertEqual(day.num_rows, 5)


class TestStaticSources(unittest.TestCase):
    def setUp(self):
        self.storage = MockStorageInterface()
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem"),
            providers=[
                ProviderConfig(
                    name="lu",
                    static=[
                        StaticConfig(
                            index_url="https://data.example.lu/gtfs/",
                            url_pattern=r"/files/gtfs-\d{8}\.zip",
                        )
                    ],
                )
            ],
        )
        self.service = StaticService(config, {"global": self.storage})
        self.zips = {}

    def _run(
        self, index_links, reuse=False, now=datetime(2026, 9, 28, 1, tzinfo=pytz.UTC)
    ):
        page = "".join(f'<a href="{link}">x</a>' for link in index_links).encode()
        requested = []

        def get_bytes(url, *args, **kwargs):
            return page

        def download(url, headers, zip_path):
            requested.append(url)
            with open(zip_path, "wb") as f:
                f.write(self.zips[url])
            return None, None

        class _Now(datetime):
            @classmethod
            def now(cls, tz=None):
                return now.astimezone(tz)

        with (
            patch("src.gtfs_rt_aggregator.static.service.get_bytes", get_bytes),
            patch.object(StaticService, "_download", staticmethod(download)),
            patch("src.gtfs_rt_aggregator.static.service.datetime", _Now),
        ):
            self.service.run_once(
                provider_name="lu",
                feed_name="static",
                url=None,
                timezone="Europe/Luxembourg",
                index_url="https://data.example.lu/gtfs/",
                url_pattern=r"/files/gtfs-\d{8}\.zip",
                retries=0,
                reuse_unchanged_tables=reuse,
            )
        return requested

    def _latest(self):
        return json.loads(self.storage.get_bytes("lu/static/latest.json"))

    def test_newest_link_used(self):
        self.zips["https://data.example.lu/files/gtfs-20260921.zip"] = _make_zip(
            GTFS_FILES
        )
        requested = self._run(
            ["/files/gtfs-20260914.zip", "/files/gtfs-20260921.zip", "/other.zip"]
        )
        self.assertEqual(requested, ["https://data.example.lu/files/gtfs-20260921.zip"])
        self.assertEqual(
            self._latest()["url"], "https://data.example.lu/files/gtfs-20260921.zip"
        )

    def test_no_match_stores_nothing(self):
        self._run(["/other.zip"])
        self.assertEqual(self.storage.list_paths("lu/"), [])

    def test_reuse_unchanged_tables(self):
        self.zips["https://data.example.lu/files/gtfs-20260921.zip"] = _make_zip(
            GTFS_FILES
        )
        self._run(["/files/gtfs-20260921.zip"], reuse=True)
        first = self._latest()

        files = dict(GTFS_FILES)
        files["stops.txt"] += "S3,Stop Three,52.39,4.91\n"
        self.zips["https://data.example.lu/files/gtfs-20260928.zip"] = _make_zip(files)
        self._run(
            ["/files/gtfs-20260928.zip"],
            reuse=True,
            now=datetime(2026, 9, 28, 2, tzinfo=pytz.UTC),
        )
        second = self._latest()

        self.assertNotEqual(first["version"], second["version"])
        self.assertEqual(second["tables"]["trips"], first["tables"]["trips"])
        self.assertTrue(
            second["tables"]["stops"].startswith(f"lu/static/{second['version']}/")
        )
        self.assertEqual(
            sorted(
                p for p in self.storage.list_paths(f"lu/static/{second['version']}/")
            ),
            [
                f"lu/static/{second['version']}/manifest.json",
                f"lu/static/{second['version']}/stops.parquet",
            ],
        )

    def test_url_or_index_required(self):
        for kwargs in (
            {},
            {"url": "u", "index_url": "i", "url_pattern": "p"},
            {"index_url": "i"},
        ):
            with self.subTest(kwargs), self.assertRaises(ValueError):
                StaticConfig(**kwargs)


if __name__ == "__main__":
    unittest.main()


class TestConvertOldFiles(unittest.TestCase):
    def test_convert(self):
        import os
        import tempfile

        from src.gtfs_rt_aggregator.aggregator.convert import convert_old_files
        from src.gtfs_rt_aggregator.storage.filesystem import FileSystemStorage

        with tempfile.TemporaryDirectory() as tmp:
            storage = FileSystemStorage(tmp)
            config = GtfsRtConfig(
                storage=StorageConfig(
                    type="filesystem", params={"base_directory": tmp}
                ),
                providers=[
                    ProviderConfig(
                        name="nl",
                        realtime=[ApiConfig(url="u", services=["VehiclePosition"])],
                    )
                ],
            )
            old = _rows(["a"], ["h"], [1742550861])
            buffer = io.BytesIO()
            pq.write_table(old, buffer)
            path = "provider=nl/service=VehiclePosition/date=2025-03-21/09-00-00_to_10-00-00.parquet"
            storage.save_bytes(buffer.getvalue(), path)

            self.assertEqual(convert_old_files(config, {"global": storage}), 1)
            table = pq.read_table(os.path.join(tmp, path))
            self.assertEqual(
                table.schema.field("fetchTime").type, pa.timestamp("us", tz="UTC")
            )
            self.assertEqual(table["provider"].to_pylist(), ["nl"])
            # Already converted: left alone
            self.assertEqual(convert_old_files(config, {"global": storage}), 0)
