"""
Failures in the worker pools, through the runtime's own submit and collect,
with real worker processes. On 7 Oct 2026 the pipeline stopped for hours: an
error the main process could not unpickle broke a pool, and its workers,
ignoring SIGTERM, never let the pool end.
"""

import os
import shutil
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

from src.gtfs_rt_aggregator.config.models import (
    ApiConfig,
    GtfsRtConfig,
    ProviderConfig,
    RuntimeConfig,
    StorageConfig,
)
from src.gtfs_rt_aggregator.runtime import worker
from src.gtfs_rt_aggregator.runtime.runtime import Runtime, Task


@worker._timed
def fail_unpicklable():
    from minio.error import ServerError

    # Its __init__ takes two arguments: cannot be unpickled
    raise ServerError("bucket unavailable", 503)


@worker._timed
def sleep(seconds):
    time.sleep(seconds)


@worker._timed
def die():
    os._exit(3)


class PoolFailuresTest(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp)
        config = GtfsRtConfig(
            storage=StorageConfig(type="filesystem", params={"base_directory": tmp}),
            providers=[
                ProviderConfig(
                    name="p",
                    realtime=[ApiConfig(url="https://x.org/a", services=["TripUpdate"])],
                )
            ],
            runtime=RuntimeConfig(spool_dir=os.path.join(tmp, "spool"), workers=2),
        )
        self.runtime = Runtime(config, {"global": MagicMock()})
        self.runtime._normal_pool = self.runtime._new_pool(2)
        self.addCleanup(self._close)
        self.finished = {}
        self.runtime._finish = lambda task, result, error, blame=True, cancelled=False: (
            self.finished.__setitem__(task.key, (result, error, blame))
        )

    def _close(self):
        for pool in (self.runtime._normal_pool,):
            pool.shutdown(wait=False, cancel_futures=True)

    def _collect_all(self, seconds=30):
        deadline = time.monotonic() + seconds
        while self.runtime._in_flight and time.monotonic() < deadline:
            self.runtime._collect()
            time.sleep(0.1)
        self.assertEqual(self.runtime._in_flight, {}, "tasks never collected")

    def test_unpicklable_error_does_not_break_the_pool(self):
        pool = self.runtime._normal_pool
        self.runtime._submit(Task("raw", "a", "normal"), fail_unpicklable)
        self._collect_all()
        result, error, blame = self.finished["a"]
        self.assertEqual(error, "ServerError: bucket unavailable")
        self.assertIs(self.runtime._normal_pool, pool)

    def test_task_past_its_deadline_is_stopped_alone_blamed(self):
        pool = self.runtime._normal_pool
        with patch.dict(
            "src.gtfs_rt_aggregator.runtime.runtime.TASK_DEADLINE_SECONDS",
            {"raw": 2},
        ):
            self.runtime._submit(Task("raw", "hung", "normal"), sleep, 60)
            time.sleep(0.5)
            self.runtime._submit(Task("window", "beside", "normal"), sleep, 60)
            started = time.monotonic()
            self._collect_all()
        self.assertLess(time.monotonic() - started, 20)
        self.assertEqual(self.finished["hung"][1:], ("stopped after 2 s", True))
        self.assertFalse(self.finished["beside"][2])
        self.assertIsNot(self.runtime._normal_pool, pool)
        # The new pool works
        self.runtime._submit(Task("raw", "after", "normal"), sleep, 0)
        self._collect_all()
        self.assertIsNone(self.finished["after"][1])

    def test_hung_task_alone_in_its_lane_is_stopped(self):
        self.runtime._heavy_pool = self.runtime._new_pool(1)
        self.addCleanup(lambda: self.runtime._heavy_pool.shutdown(wait=False))
        with patch.dict(
            "src.gtfs_rt_aggregator.runtime.runtime.TASK_DEADLINE_SECONDS",
            {"compact": 2},
        ):
            self.runtime._submit(Task("compact", "hung", "heavy"), sleep, 60)
            started = time.monotonic()
            self._collect_all()
        self.assertLess(time.monotonic() - started, 20)
        self.assertEqual(self.finished["hung"][1], "stopped after 2 s")

    def test_dead_worker_fails_its_pool_without_blocking(self):
        pool = self.runtime._normal_pool
        self.runtime._submit(Task("window", "busy", "normal"), sleep, 60)
        time.sleep(0.5)
        self.runtime._submit(Task("raw", "dies", "normal"), die)
        time.sleep(1)
        # As the pipeline always does: a pool whose worker started with a
        # submit notices its death at the next submit or result (CPython wakes
        # its manager thread before starting the worker)
        self.runtime._submit(Task("raw", "next", "normal"), sleep, 0)
        started = time.monotonic()
        self._collect_all()
        self.assertLess(time.monotonic() - started, 20)
        for key in ("busy", "dies"):
            result, error, blame = self.finished[key]
            self.assertIn("worker process died", error)
            self.assertTrue(blame)
        self.assertIsNot(self.runtime._normal_pool, pool)
        for process in pool._processes.values() if pool._processes else ():
            process.join(timeout=10)
            self.assertFalse(process.is_alive())


if __name__ == "__main__":
    unittest.main()
