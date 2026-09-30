"""Adapters: Python functions as the source of realtime and static feeds."""

import json
import logging
import shutil
import signal
import tempfile
import textwrap
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow.parquet as pq
from pydantic import ValidationError

from src.gtfs_rt_aggregator.config.loader import (
    load_config_from_toml,
    load_config_from_toml_file,
)
from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    ProviderConfig,
    RuntimeConfig,
    StaticConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.runtime import worker
from src.gtfs_rt_aggregator.runtime.core import feed_hash, feed_id
from src.gtfs_rt_aggregator.runtime.runtime import Runtime, Task
from src.gtfs_rt_aggregator.runtime.spool import Spool
from tests.unit.test_static_service import GTFS_FILES, _make_zip

DATA = Path(__file__).parent.parent / "data"

ADAPTER = textwrap.dedent("""
    from pathlib import Path

    from google.transit import gtfs_realtime_pb2

    DATA = Path({data!r})


    def fetch(state, env):
        if env.get("ADAPTER_FAIL"):
            raise RuntimeError("API down")
        state["polls"] = state.get("polls", 0) + 1
        message = gtfs_realtime_pb2.FeedMessage()
        message.ParseFromString((DATA / "vehicle_positions.pb").read_bytes())
        del message.entity[10:]
        message.header.timestamp += state["polls"]
        return message


    def same(state, env):
        return b"constant bytes"


    def timetable(state, env, out_dir):
        state["builds"] = state.get("builds", 0) + 1
        path = Path(out_dir) / "gtfs.zip"
        path.write_bytes(ZIP)
        return path


    def timetable_folder(state, env, out_dir):
        import io, zipfile

        folder = Path(out_dir) / "gtfs"
        folder.mkdir()
        zipfile.ZipFile(io.BytesIO(ZIP)).extractall(folder)
        return folder
    """)


def _write_adapter(folder: Path) -> Path:
    path = folder / "adapters" / "fake.py"
    path.parent.mkdir(parents=True)
    source = ADAPTER.format(data=str(DATA)) + f"\nZIP = {_make_zip(GTFS_FILES)!r}\n"
    path.write_text(source)
    return path


class TestConfig(unittest.TestCase):
    def test_url_or_adapter(self):
        with self.assertRaises(ValidationError):
            ApiConfig(url="u", adapter="a.py:f", services=["Alert"])
        with self.assertRaises(ValidationError):
            ApiConfig(services=["Alert"])
        with self.assertRaises(ValidationError):
            ApiConfig(adapter="no-function", services=["Alert"])
        with self.assertRaises(ValidationError):
            StaticConfig(url="https://x.org/a.zip", adapter="a.py:f")
        StaticConfig(adapter="a.py:timetable")

    def test_feed_hash_from_spec(self):
        api = ApiConfig(adapter="adapters/tv.py:fetch", services=["TripUpdate"])
        other = ApiConfig(adapter="adapters/tv.py:other", services=["TripUpdate"])
        self.assertNotEqual(feed_hash(api), feed_hash(other))
        self.assertEqual(api.source, "adapter adapters/tv.py:fetch")

    def test_paths_relative_to_the_config_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp, "config.toml")
            path.write_text(
                '[storage]\ntype = "filesystem"\n'
                '[[providers]]\nname = "se"\n'
                "  [[providers.realtime]]\n"
                '  adapter = "adapters/fake.py:fetch"\n'
                '  services = ["VehiclePosition"]\n'
            )
            config = load_config_from_toml(path)
            self.assertEqual(config.base_dir, str(Path(tmp).resolve()))
            # Without a file: the working directory
            config = load_config_from_toml_file(BytesIO(path.read_bytes()))
            self.assertIsNone(config.base_dir)


class _WorkerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        _write_adapter(self.tmp)
        self._signals = {
            number: signal.getsignal(number)
            for number in (signal.SIGINT, signal.SIGTERM)
        }

    def tearDown(self):
        worker._CTX = None
        for number, handler in self._signals.items():
            signal.signal(number, handler)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _config(self, realtime=(), static=()):
        return GtfsRtConfig(
            storage=StorageConfig(
                type="filesystem", params={"base_directory": str(self.tmp / "out")}
            ),
            providers=[
                ProviderConfig(
                    name="se",
                    timezone="Europe/Stockholm",
                    realtime=list(realtime),
                    static=list(static),
                )
            ],
            runtime=RuntimeConfig(spool_dir=str(self.tmp / "spool")),
            base_dir=str(self.tmp),
        )

    def _start(self, config):
        # A (re)started worker process
        worker._CTX = None
        worker.init_worker(config, str(self.tmp / "spool"), logging.INFO)


class TestRealtimeAdapter(_WorkerTest):
    def setUp(self):
        super().setUp()
        self.api = ApiConfig(
            adapter="adapters/fake.py:fetch",
            services=["VehiclePosition"],
            skip_unchanged=False,
        )
        self.config = self._config(realtime=[self.api])
        self.feed = feed_id("se", self.api)
        self.spool = Spool(str(self.tmp / "spool"))

    def _state(self):
        path = self.spool.path("state", f"{self.feed}.adapter.json")
        return json.loads(path.read_text())["state"]

    def test_fetch_goes_to_the_spool_and_state_persists(self):
        self._start(self.config)
        first = worker.adapter_fetch(self.feed, None)
        worker.adapter_fetch(self.feed, first["sha256"])
        self.assertEqual(self._state(), {"polls": 2})
        self.assertEqual(len(self.spool.queued_items()[self.feed]), 2)

        # A restart: the adapter goes on from its saved state
        self._start(self.config)
        worker.adapter_fetch(self.feed, None)
        self.assertEqual(self._state(), {"polls": 3})

        # Processed like a download: rows with the feed id of the adapter spec
        item = self.spool.claim(self.spool.queued_items()[self.feed][0])
        (path,) = worker.process_item(str(item))["written"]
        table = pq.read_table(self.spool.ready_path("se", path))
        self.assertEqual(table.num_rows, 10)
        self.assertEqual(set(table["feedId"].to_pylist()), {feed_hash(self.api)})

    def test_failure_keeps_the_state(self):
        import os
        from unittest.mock import patch

        self._start(self.config)
        worker.adapter_fetch(self.feed, None)
        with patch.dict(os.environ, {"ADAPTER_FAIL": "1"}):
            with self.assertRaisesRegex(RuntimeError, "API down"):
                worker.adapter_fetch(self.feed, None)
        self.assertEqual(self._state(), {"polls": 1})

    def test_unchanged_bytes_not_stored(self):
        api = ApiConfig(adapter="adapters/fake.py:same", services=["VehiclePosition"])
        config = self._config(realtime=[api])
        feed = feed_id("se", api)
        self._start(config)
        first = worker.adapter_fetch(feed, None)
        second = worker.adapter_fetch(feed, first["sha256"])
        self.assertTrue(second["unchanged"])
        self.assertEqual(len(self.spool.queued_items()[feed]), 1)

    def test_runtime_records_failures_and_fetches(self):
        runtime = Runtime(self.config, {"global": MagicMock()})
        task = Task("adapter", f"adapter {self.feed}", "normal", {"feed": self.feed})
        runtime._fetching.add(self.feed)
        runtime._finish(task, None, "RuntimeError: API down")
        status = runtime._status[self.feed]
        self.assertEqual(status["last_error"], "RuntimeError: API down")
        self.assertNotIn(self.feed, runtime._fetching)

        item = str(self.spool.path("incoming", self.feed, "20260930T080000.000000Z.pb"))
        runtime._finish(
            task,
            {
                "fetch_time": "2026-09-30T10:00:00+02:00",
                "size": 3,
                "sha256": "abc",
                "item": item,
            },
            None,
        )
        self.assertEqual(runtime._queues[self.feed], [Path(item)])
        self.assertEqual(runtime._last_sha[self.feed], "abc")
        self.assertEqual(runtime._status[self.feed]["size"], 3)


class TestStaticAdapter(_WorkerTest):
    def _latest(self):
        return json.loads(
            Path(self.tmp, "out", "se", "static", "latest.json").read_text()
        )

    def test_zip_stored_as_a_version(self):
        static = StaticConfig(adapter="adapters/fake.py:timetable", check_minutes=60)
        self._start(self._config(static=[static]))
        worker.static_adapter("se", "static", True)
        latest = self._latest()
        self.assertEqual(latest["url"], "adapter:adapters/fake.py:timetable")
        self.assertIn("stop_times", latest["tables"])

        # First check after a restart: ran less than check_minutes ago, skipped
        self._start(self._config(static=[static]))
        self.assertTrue(worker.static_adapter("se", "static", True)["skipped"])
        # Later checks run it (same content: no new version)
        worker.static_adapter("se", "static", False)
        self.assertEqual(self._latest()["version"], latest["version"])
        state = Path(self.tmp, "spool", "state", "se__static__static.adapter.json")
        self.assertEqual(json.loads(state.read_text())["state"], {"builds": 2})

    def test_folder_is_zipped(self):
        static = StaticConfig(adapter="adapters/fake.py:timetable_folder")
        self._start(self._config(static=[static]))
        worker.static_adapter("se", "static", True)
        self.assertIn("trips", self._latest()["tables"])


if __name__ == "__main__":
    unittest.main()
