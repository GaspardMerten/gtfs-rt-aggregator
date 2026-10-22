"""
The adapter lane (0.12.4): realtime adapter polls run in one long-lived
process, a thread each, never in the workers that process fetches and close
windows. On 22 Oct 2026 polls waiting on slow APIs held both normal workers
and the windows of 15 feeds stayed unclosed for hours.
"""

import os
import shutil
import signal
import tempfile
import textwrap
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.gtfs_rt_aggregator import adapters
from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    ProviderConfig,
    RuntimeConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.runtime.core import feed_id
from src.gtfs_rt_aggregator.runtime.runtime import Runtime

ADAPTER = textwrap.dedent("""
    import os
    import time

    _calls = {}  # module state, kept between polls


    def _poll(name):
        _calls[name] = _calls.get(name, 0) + 1
        return f"{os.getpid()}:{_calls[name]}".encode()


    def slow_a(state, env):
        time.sleep(float(env.get("ADAPTER_SLEEP", "1")))
        return _poll("a")


    def slow_b(state, env):
        time.sleep(float(env.get("ADAPTER_SLEEP", "1")))
        return _poll("b")


    def hang(state, env):
        time.sleep(60)
        return b"never"


    def die(state, env):
        os._exit(3)
    """)


class AdapterLaneTest(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        path = self.tmp / "adapters" / "lane.py"
        path.parent.mkdir()
        path.write_text(ADAPTER)
        self._signals = {
            number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)
        }
        self.addCleanup(self._restore_signals)

    def _restore_signals(self):
        for number, handler in self._signals.items():
            signal.signal(number, handler)

    def _runtime(self, functions, **runtime):
        apis = [
            ApiConfig(
                adapter=f"adapters/lane.py:{name}",
                services=["VehiclePosition"],
                skip_unchanged=False,
            )
            for name in functions
        ]
        config = GtfsRtConfig(
            storage=StorageConfig(
                type="filesystem", params={"base_directory": str(self.tmp / "out")}
            ),
            providers=[
                ProviderConfig(name="se", timezone="Europe/Stockholm", realtime=apis)
            ],
            runtime=RuntimeConfig(spool_dir=str(self.tmp / "spool"), **runtime),
            base_dir=str(self.tmp),
        )
        rt = Runtime(config, {"global": MagicMock()})
        rt._adapter_pool = rt._new_adapter_pool()
        self.addCleanup(self._stop, rt)
        return rt, [feed_id("se", api) for api in apis]

    @staticmethod
    def _stop(rt):
        for pool in [rt._adapter_pool, *rt._retired_pools]:
            if pool is not None:
                pool.shutdown()

    @staticmethod
    def _wait(rt, timeout=30):
        deadline = time.monotonic() + timeout
        while rt._fetching and time.monotonic() < deadline:
            rt._collect()
            time.sleep(0.05)
        rt._collect()
        assert not rt._fetching, rt._fetching

    @staticmethod
    def _polls(rt, feed):
        """What each stored poll of feed returned ("<pid>:<call>")."""
        return [item.read_bytes().decode() for item in rt._queues.get(feed, [])]

    def test_polls_wait_side_by_side_and_module_state_stays(self):
        rt, (a, b) = self._runtime(["slow_a", "slow_b"])
        rt._normal_pool = MagicMock()  # never used by a poll
        started = time.monotonic()
        rt._submit_fetch(a)
        rt._submit_fetch(b)
        # A feed is polled once at a time: the turn is skipped, not queued
        rt._submit_fetch(a)
        self.assertEqual(rt._status[a]["skipped_overlap"], 1)
        self._wait(rt)
        # Both waited at once (sequential would be ≥ 2 s)
        self.assertLess(time.monotonic() - started, 1.9)
        rt._normal_pool.submit.assert_not_called()

        rt._submit_fetch(a)
        self._wait(rt)
        first, second = self._polls(rt, a)
        # The same process, which kept the module's state between polls
        self.assertEqual(first.split(":")[0], second.split(":")[0])
        self.assertEqual([first[-1], second[-1]], ["1", "2"])
        self.assertNotEqual(first.split(":")[0], str(os.getpid()))

    def test_poll_past_its_deadline_stops_the_process(self):
        rt, (hang, a) = self._runtime(["hang", "slow_a"])
        old = rt._adapter_pool
        with patch.dict(
            "src.gtfs_rt_aggregator.runtime.runtime.TASK_DEADLINE_SECONDS",
            {"adapter": 1},
        ), patch.dict(os.environ, {"ADAPTER_SLEEP": "30"}):
            rt._submit_fetch(hang)
            time.sleep(0.5)
            rt._submit_fetch(a)
            with self.assertLogs("src.gtfs_rt_aggregator.runtime.runtime", "WARNING") as logs:
                self._wait(rt)
        self.assertIsNot(rt._adapter_pool, old)
        self.assertFalse(old._process.is_alive())
        # The poll past its deadline failed; the other one was only stopped with it
        self.assertIn("stopped after 1 s", rt._status[hang]["last_error"])
        self.assertNotIn("last_error", rt._status.get(a, {}))
        self.assertTrue(any("killing the adapter workers" in line for line in logs.output))

        # The new process takes the next polls
        with patch.dict(os.environ, {"ADAPTER_SLEEP": "0"}):
            rt._submit_fetch(a)
            self._wait(rt)
        self.assertEqual(len(self._polls(rt, a)), 1)

    def test_process_that_dies_is_replaced(self):
        rt, (die, a) = self._runtime(["die", "slow_a"])
        old = rt._adapter_pool
        with self.assertLogs("src.gtfs_rt_aggregator.runtime.runtime", "ERROR"):
            rt._submit_fetch(die)
            self._wait(rt)
        self.assertIn("worker process died", rt._status[die]["last_error"])
        self.assertIsNot(rt._adapter_pool, old)
        with patch.dict(os.environ, {"ADAPTER_SLEEP": "0"}):
            rt._submit_fetch(a)
            self._wait(rt)
        self.assertEqual(len(self._polls(rt, a)), 1)

    def test_replaced_for_its_memory_once_its_polls_end(self):
        rt, (a, b) = self._runtime(["slow_a", "slow_b"], adapter_max_memory_mb=1)
        old = rt._adapter_pool
        with patch.dict(os.environ, {"ADAPTER_SLEEP": "0.2"}):
            rt._submit_fetch(a)
            with self.assertLogs("src.gtfs_rt_aggregator.runtime.runtime", "WARNING"):
                self._wait(rt)
        self.assertIsNot(rt._adapter_pool, old)
        self.assertEqual(rt._retired_pools, [])
        self.assertFalse(old._process.is_alive())
        self.assertNotIn("last_error", rt._status[a])


class LoadOnceTest(unittest.TestCase):
    def test_threads_loading_one_file_get_one_module(self):
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "once.py").write_text("import time\ntime.sleep(0.2)\ndef f(state, env):\n    return b''\n")
            loaded = []
            threads = [
                threading.Thread(target=lambda: loaded.append(adapters.load("once.py:f", tmp)))
                for _ in range(4)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(len({id(function) for function in loaded}), 1)


if __name__ == "__main__":
    unittest.main()
