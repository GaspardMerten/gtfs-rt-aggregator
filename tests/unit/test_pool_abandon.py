"""A broken worker pool is left without blocking (the pipeline stopped for hours on 7 Oct 2026)."""

import os
import signal
import threading
import time
import unittest
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool

from src.gtfs_rt_aggregator.runtime.runtime import _abandon_pool, _pool_context


def _ignore_sigterm():
    # As worker.init_worker does
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


def _die():
    os._exit(3)


def _busy():
    time.sleep(60)


class AbandonPoolTest(unittest.TestCase):
    def test_broken_pool_with_workers_ignoring_sigterm(self):
        pool = ProcessPoolExecutor(max_workers=2, mp_context=_pool_context(), initializer=_ignore_sigterm)
        busy = pool.submit(_busy)
        time.sleep(1)  # the busy task holds one worker
        died = pool.submit(_die)
        processes = list(pool._processes.values())
        deadline = time.monotonic() + 30
        while all(p.exitcode is None for p in processes) and time.monotonic() < deadline:
            time.sleep(0.1)
        # The pool's own clean-up now sends SIGTERM to the busy worker, which ignores it, and waits for it with
        # its shutdown lock held: neither future fails, and shutdown() would block
        time.sleep(1)
        self.assertFalse(died.done())
        done = threading.Event()
        threading.Thread(target=lambda: (_abandon_pool(pool, "test"), done.set()), daemon=True).start()
        self.assertTrue(done.wait(10), "_abandon_pool blocked")
        for future in (busy, died):
            with self.assertRaises(BrokenProcessPool):
                future.result(timeout=10)
        for process in processes:
            process.join(timeout=10)
            self.assertFalse(process.is_alive())


class PortableErrorTest(unittest.TestCase):
    def test_unpicklable_error_becomes_runtime_error(self):
        from minio.error import ServerError

        from src.gtfs_rt_aggregator.runtime.worker import _portable

        error = _portable(ServerError("bucket unavailable", 503))
        self.assertIsInstance(error, RuntimeError)
        self.assertIn("ServerError: bucket unavailable", str(error))
        kept = ValueError("x")
        self.assertIs(_portable(kept), kept)
