"""Environment variables, output paths, deduplication, compaction, static URLs."""

import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from io import BytesIO
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from src.gtfs_rt_aggregator.aggregator.dedup import deduplicate, max_gap_seconds
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
    def test_feeds_deduplicated_apart(self):
        # Two feeds of one service (e.g. two operators) fetched at different
        # times, with the same entity id: each feed's unchanged entity is one
        # row; the other feed's fetch times are not gaps
        table = _rows(["1"] * 6, ["x", "y"] * 3, [10, 11, 20, 21, 30, 31])
        table = table.append_column("feedId", pa.array(["a", "b"] * 3))
        result = deduplicate(table)
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(sorted(result["lastSeen"].to_pylist()), [30, 31])

        # Streaming compaction passes fetch times by feed
        result = deduplicate(
            table,
            {
                "a": pa.array([10, 20, 30], pa.uint64()),
                "b": pa.array([11, 21, 31], pa.uint64()),
            },
        )
        self.assertEqual(result.num_rows, 2)

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

    def test_outage_ends_run(self):
        # Fetched every minute, then not at all for 7 hours (the pipeline
        # was down): the entity is not known to have been there all along
        minutes = [0, 1, 2, 7 * 60 + 2, 7 * 60 + 3]
        table = _timed_rows(["a"] * 5, ["x"] * 5, minutes)
        runs = deduplicate(table)
        self.assertEqual(
            _spans(runs), [(0, 2), (7 * 60 + 2, 7 * 60 + 3)]
        )

        # Read every minute but unchanged (no rows written): one run
        unchanged = _minutes(range(3, 7 * 60 + 2))
        runs = deduplicate(table, unchanged=unchanged)
        self.assertEqual(_spans(runs), [(0, 7 * 60 + 3)])

        # The longest wait follows the feed's refresh_seconds: fetched every
        # 5 minutes, a 15-minute wait is no outage, a 16-minute one is
        table = _timed_rows(["a"] * 3, ["x"] * 3, [0, 15, 31])
        self.assertEqual(
            _spans(deduplicate(table, max_gap=max_gap_seconds(300))), [(0, 15), (31, 31)]
        )
        self.assertEqual(
            _spans(deduplicate(table, max_gap={None: max_gap_seconds(600)})), [(0, 31)]
        )

    def test_unchanged_fetch_is_no_gap(self):
        # Unlike a fetch without the entity, an unchanged fetch had all of them
        table = _timed_rows(["a", "a"], ["x", "x"], [0, 2])
        self.assertEqual(_spans(deduplicate(table, unchanged=_minutes([1]))), [(0, 2)])
        self.assertEqual(
            _spans(deduplicate(table, times=_minutes([1]))), [(0, 0), (2, 2)]
        )

    def test_trip_updates_told_apart_by_start_date(self):
        # One entity id for the same trip on two days, in every fetch
        table = _timed_rows(["a"] * 4, ["d1", "d2", "d1", "d2"], [0, 0, 1, 1])
        table = table.append_column("trip_tripId", pa.array(["T"] * 4))
        table = table.append_column(
            "trip_startDate", pa.array(["20261004", "20261005"] * 2)
        )
        result = deduplicate(table)
        self.assertEqual(result.num_rows, 2)
        self.assertEqual(sorted(result["trip_startDate"].to_pylist()), ["20261004", "20261005"])
        self.assertEqual(result["lastSeen"].to_pylist(), [_minutes([1])[0].as_py()] * 2)


_START = datetime(2026, 10, 4, 6, 0, tzinfo=timezone.utc)


def _minutes(minutes):
    return pa.array(
        [_START + timedelta(minutes=m) for m in minutes], pa.timestamp("us", tz="UTC")
    )


def _timed_rows(entity_ids, hashes, minutes):
    return pa.table(
        {
            "entityId": entity_ids,
            "contentHash": hashes,
            "fetchTime": _minutes(minutes),
        }
    )


def _spans(table):
    """(firstSeen, lastSeen) of each row, in minutes after _START."""
    def minute(value):
        return round((value - _START).total_seconds() / 60)

    return sorted(
        (minute(first), minute(last))
        for first, last in zip(
            table["firstSeen"].to_pylist(), table["lastSeen"].to_pylist()
        )
    )


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
