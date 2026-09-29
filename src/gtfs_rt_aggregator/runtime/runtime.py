"""
The pipeline's runtime:

    scheduler ─▶ fetch threads ─▶ spool/incoming ─▶ worker processes ─▶ spool/ready ─▶ upload thread ─▶ storage
                                                     (normal + heavy)

- Fetch threads only download, streaming to disk: a fetch is never skipped
  or lost because processing is slow, and never held in memory.
- A fixed pool of long-lived worker processes processes fetches, one at a time
  per feed and in fetch order. Memory-heavy work (large fetches, static feeds,
  aggregation, compaction) goes to a separate heavy pool (1 process by default).
- A failed or killed item is retried, then quarantined after max_attempts.
- The upload thread moves results to storage; during an outage they wait on disk.
"""

import logging
import multiprocessing
import os
import random
import shutil
import signal
import tempfile
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pytz

from ..aggregator.service import AggregatorService
from ..config.models import GtfsRtConfig
from ..runtime import worker
from ..runtime.core import feed_slug, realtime_feeds
from ..runtime.spool import Spool, item_name, read_json, write_json
from ..static.service import StaticService
from ..utils.cleanup import clean_stale_temp_files
from ..utils.http import download_to
from ..utils.redact import redact, strip_query

logger = logging.getLogger(__name__)

GLOBAL = "__global__"  # ready/ folder of files for the global storage
STATUS_EVERY_SECONDS = 10
INDEX_EVERY_SECONDS = 30
WINDOWS_EVERY_SECONDS = 5
RAW_EVERY_SECONDS = 60
SPOOL_SIZE_EVERY_SECONDS = 10
# A raw hour is bundled this long after it ended (late fetches)
RAW_GRACE_SECONDS = 120


def default_spool_dir() -> str:
    return os.path.join(tempfile.gettempdir(), "gtfs_rt_aggregator-spool")


@dataclass
class Job:
    """Something to run every interval seconds."""

    name: str
    interval: float
    run: Callable[[], None]
    next_run: float = 0.0


@dataclass
class Task:
    """Work submitted to a worker pool."""

    kind: str
    key: str
    lane: str
    info: Dict = field(default_factory=dict)


def _pool_context():
    methods = multiprocessing.get_all_start_methods()
    if "forkserver" in methods:
        ctx = multiprocessing.get_context("forkserver")
        # Loaded once in the fork server, shared by every worker it forks
        ctx.set_forkserver_preload(
            [
                "pyarrow",
                "pyarrow.parquet",
                "google.transit.gtfs_realtime_pb2",
                "gtfs_rt_aggregator.runtime.worker",
            ]
        )
        return ctx
    return multiprocessing.get_context("spawn")


class Runtime:
    def __init__(self, config: GtfsRtConfig, storages=None):
        from ..pipeline import create_storages

        self.config = config
        self.runtime = config.runtime
        self.storages = storages or create_storages(config)
        self.spool = Spool(self.runtime.spool_dir or default_spool_dir())
        self.feeds = realtime_feeds(config)
        self.statics = {
            f"{provider.name}__static__{feed.name}": (provider, feed)
            for provider in config.providers
            for feed in provider.static
        }
        self.static_service = (
            StaticService(config, self.storages) if self.statics else None
        )

        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._jobs: List[Job] = []
        self._fetching: set = set()  # feeds (realtime and static) being downloaded
        self._busy_feeds: set = set()  # realtime feeds with an item in a worker
        self._heavy_queue: List[Task] = []  # timed heavy tasks waiting for a slot
        self._in_flight: Dict[Future, Task] = {}
        self._keys_in_flight: set = set()
        self._last_sha: Dict[str, str] = {}
        self._status: Dict[str, Dict] = {}
        self._status_dirty: set = set()
        self._paused: set = set()
        self._spool_size = 0
        self._log_level = logging.getLogger().getEffectiveLevel()

    # Lifecycle ---------------------------------------------------------------

    def run(self):
        """Run until stop() is called, or SIGTERM / Ctrl+C."""
        previous = self._handle_sigterm()
        try:
            self._start()
            logger.info("Pipeline started. Press Ctrl+C to stop.")
            while not self._stop.is_set():
                self._tick()
                self._stop.wait(0.2)
        except KeyboardInterrupt:
            logger.info("Stopping (keyboard interrupt)")
        finally:
            self._shutdown()
            if previous is not None:
                signal.signal(signal.SIGTERM, previous)

    def stop(self):
        self._stop.set()

    def _handle_sigterm(self):
        main_pid = os.getpid()

        def handler(signum, frame):
            if os.getpid() != main_pid:
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                os.kill(os.getpid(), signal.SIGTERM)
                return
            logger.info("Stopping (SIGTERM)")
            self._stop.set()

        try:
            return signal.signal(signal.SIGTERM, handler)
        except ValueError:  # not the main thread
            return None

    def _start(self):
        removed = clean_stale_temp_files()
        if removed:
            logger.info(f"Removed {removed} stale temporary files or folders")
        recovered = self.spool.recover(self.runtime.max_attempts)
        if any(recovered.values()):
            logger.info(f"Spool recovered after restart: {recovered}")

        self._fetch_pool = ThreadPoolExecutor(
            self.runtime.fetch_threads, thread_name_prefix="fetch"
        )
        self._normal_pool = self._new_pool(self.runtime.worker_count())
        self._heavy_pool = self._new_pool(self.runtime.heavy_slots)
        self._uploader = Uploader(self.spool, self.storages, self._stop)
        self._uploader.start()
        self._schedule_jobs()

    def _new_pool(self, workers: int) -> ProcessPoolExecutor:
        return ProcessPoolExecutor(
            max_workers=workers,
            mp_context=_pool_context(),
            initializer=worker.init_worker,
            initargs=(self.config, str(self.spool.root), self._log_level),
            max_tasks_per_child=self.runtime.max_tasks_per_worker,
        )

    def _shutdown(self):
        logger.info("Shutting down: waiting for running downloads and tasks")
        self._stop.set()
        self._fetch_pool.shutdown(wait=True, cancel_futures=True)
        # Running tasks finish; queued ones stay on disk for the next start
        deadline = time.monotonic() + 30
        while self._in_flight and time.monotonic() < deadline:
            self._collect()
            time.sleep(0.2)
        for pool in (self._normal_pool, self._heavy_pool):
            pool.shutdown(wait=False, cancel_futures=True)
        self._uploader.drain(timeout=30)
        self._write_statuses(force=True)
        self._write_index()
        self._uploader.drain(timeout=10)
        logger.info("Stopped")

    # Scheduling --------------------------------------------------------------

    def _schedule_jobs(self):
        now = time.monotonic()
        jitter = self.runtime.startup_jitter_seconds

        def add(name, interval, run):
            first = now + random.uniform(0, min(interval, jitter)) if jitter else now
            self._jobs.append(Job(name, interval, run, first))

        for feed, (provider, api) in self.feeds.items():
            add(
                f"fetch {feed}",
                api.refresh_seconds,
                lambda feed=feed: self._submit_fetch(feed),
            )
        for feed in self.statics:
            _, static = self.statics[feed]
            add(
                f"static {feed}",
                static.check_minutes * 60,
                lambda feed=feed: self._submit_static_download(feed),
            )

        aggregator = AggregatorService(self.config, self.storages)
        for seconds, func, name, args, *_ in aggregator.get_scheduling():
            kind = "compact" if func.__name__ == "compact_once" else "aggregate"
            add(
                name,
                seconds,
                lambda kind=kind, name=name, args=args: self._queue_heavy(
                    kind, name, args
                ),
            )

        # Housekeeping, in this thread: cheap
        self._jobs += [
            Job("windows", WINDOWS_EVERY_SECONDS, self._close_windows, now),
            Job("raw", RAW_EVERY_SECONDS, self._bundle_raw, now),
            Job("spool size", SPOOL_SIZE_EVERY_SECONDS, self._check_spool_size, now),
            Job(
                "status",
                STATUS_EVERY_SECONDS,
                self._write_statuses,
                now + STATUS_EVERY_SECONDS,
            ),
            Job(
                "index",
                INDEX_EVERY_SECONDS,
                self._write_index,
                now + INDEX_EVERY_SECONDS,
            ),
        ]

    def _tick(self):
        now = time.monotonic()
        for job in self._jobs:
            if now >= job.next_run:
                job.next_run = now + job.interval
                try:
                    job.run()
                except Exception as e:
                    logger.error(
                        f"Error in {job.name}: {redact(str(e))}", exc_info=True
                    )
        self._collect()
        self._dispatch()

    # Fetch lane ----------------------------------------------------------------

    def _submit_fetch(self, feed: str):
        provider, api = self.feeds[feed]
        with self._lock:
            if feed in self._paused:
                self._update_status(
                    feed,
                    skipped_spool_full=self._status.get(feed, {}).get(
                        "skipped_spool_full", 0
                    )
                    + 1,
                )
                return
            if feed in self._fetching:
                logger.warning(
                    f"{feed}: previous download still running, fetch skipped"
                )
                self._update_status(
                    feed,
                    skipped_overlap=self._status.get(feed, {}).get("skipped_overlap", 0)
                    + 1,
                )
                return
            self._fetching.add(feed)
        self._fetch_pool.submit(self._fetch, feed)

    def _fetch(self, feed: str):
        provider, api = self.feeds[feed]
        fetch_time = datetime.now(pytz.timezone(provider.timezone))
        status = {"last_attempt": fetch_time.isoformat()}
        item = self.spool.new_item(feed, fetch_time)
        tmp = item.with_name(item.name + ".part")
        try:
            size, sha256 = download_to(
                api.url, api.headers, str(tmp), api.retries, logger
            )
            status.update(last_success=fetch_time.isoformat(), size=size)
            if api.skip_unchanged and self._last_sha.get(feed) == sha256:
                # Byte for byte the previous fetch: nothing to process
                tmp.unlink(missing_ok=True)
                status["unchanged"] = True
                return
            self._last_sha[feed] = sha256
            self.spool.commit_item(
                item,
                tmp,
                {
                    "feed": feed,
                    "provider": provider.name,
                    "url": strip_query(api.url),
                    "services": api.services,
                    "fetch_time": fetch_time.isoformat(),
                    "size": size,
                    "sha256": sha256,
                    "attempt": 1,
                },
            )
        except Exception as e:
            tmp.unlink(missing_ok=True)
            status.update(
                last_error=redact(str(e)), last_error_at=fetch_time.isoformat()
            )
            logger.error(f"Fetching {redact(api.url)} failed: {redact(str(e))}")
        finally:
            with self._lock:
                self._fetching.discard(feed)
                self._update_status(feed, **status)

    def _submit_static_download(self, feed: str):
        with self._lock:
            waiting = any(self.spool.path("static", feed).glob("*/meta.json"))
            if (
                feed in self._fetching
                or waiting
                or f"static {feed}" in self._keys_in_flight
            ):
                return
            if self._paused:
                return  # spool full: no large downloads
            self._fetching.add(feed)
        self._fetch_pool.submit(self._download_static, feed)

    def _download_static(self, feed: str):
        provider, static = self.statics[feed]
        folder = self.spool.path("static", feed, item_name(datetime.now(timezone.utc)))
        folder.mkdir(parents=True, exist_ok=True)
        try:
            meta = self.static_service.download(
                provider.name,
                static.name,
                static.url,
                provider.timezone,
                str(folder),
                static.headers,
                static.index_url,
                static.url_pattern,
                static.retries,
                logger,
            )
            if meta is None:
                shutil.rmtree(folder, ignore_errors=True)
            else:
                # meta.json marks the download complete
                write_json(folder / "meta.json", {**meta, "attempt": 1})
        except Exception as e:
            shutil.rmtree(folder, ignore_errors=True)
            logger.error(f"Downloading static feed {feed} failed: {redact(str(e))}")
        finally:
            with self._lock:
                self._fetching.discard(feed)

    # Process lane ----------------------------------------------------------------

    def _dispatch(self):
        """Give waiting work to free worker slots, one item per feed at a time."""
        normal_free = self.runtime.worker_count() * 2 - self._count("normal")
        heavy_free = self.runtime.heavy_slots - self._count("heavy")
        threshold = self.runtime.heavy_threshold_mb * 1024 * 1024

        # Realtime fetches, the oldest waiting feed first
        candidates = []
        for feed in self.spool.feeds_with_pending():
            if feed in self._busy_feeds or feed not in self.feeds:
                continue
            items = self.spool.pending(feed)
            if items:
                candidates.append((items[0].name, feed, items[0]))
        for _, feed, item in sorted(candidates):
            heavy = item.stat().st_size > threshold
            if heavy and heavy_free <= 0 or not heavy and normal_free <= 0:
                continue
            claimed = self.spool.claim(item)
            lane = "heavy" if heavy else "normal"
            self._submit(
                Task("item", feed, lane, {"item": str(claimed)}),
                worker.process_item,
                str(claimed),
            )
            self._busy_feeds.add(feed)
            if heavy:
                heavy_free -= 1
            else:
                normal_free -= 1

        # Static feeds downloaded, then timed heavy tasks
        for feed in self.statics:
            if heavy_free <= 0:
                break
            key = f"static {feed}"
            if key in self._keys_in_flight:
                continue
            for meta_path in sorted(
                self.spool.path("static", feed).glob("*/meta.json")
            ):
                provider, static = self.statics[feed]
                self._submit(
                    Task("static", key, "heavy", {"folder": str(meta_path.parent)}),
                    worker.process_static,
                    provider.name,
                    static.name,
                    str(meta_path.parent),
                    static.reuse_unchanged_tables,
                )
                heavy_free -= 1
                break
        while heavy_free > 0 and self._heavy_queue:
            task = self._heavy_queue.pop(0)
            function = worker.compact if task.kind == "compact" else worker.aggregate
            self._submit(task, function, **task.info)
            heavy_free -= 1

    def _queue_heavy(self, kind: str, name: str, args: Dict):
        if name in self._keys_in_flight or any(
            t.key == name for t in self._heavy_queue
        ):
            return
        self._heavy_queue.append(Task(kind, name, "heavy", dict(args)))

    def _submit(self, task: Task, function, *args, **kwargs):
        pool = self._heavy_pool if task.lane == "heavy" else self._normal_pool
        try:
            future = pool.submit(function, *args, **kwargs)
        except BrokenProcessPool:
            self._replace_pool(task.lane)
            pool = self._heavy_pool if task.lane == "heavy" else self._normal_pool
            future = pool.submit(function, *args, **kwargs)
        self._in_flight[future] = task
        self._keys_in_flight.add(task.key)
        logger.debug(
            f"Submitted {task.kind} {task.key} to the {task.lane} pool {task.info}"
        )

    def _count(self, lane: str) -> int:
        return sum(1 for task in self._in_flight.values() if task.lane == lane)

    def _collect(self):
        """Handle finished tasks."""
        broken = set()
        for future in [f for f in self._in_flight if f.done()]:
            task = self._in_flight.pop(future)
            self._keys_in_flight.discard(task.key)
            try:
                result = future.result()
                error = None
            except BrokenProcessPool as e:
                broken.add(task.lane)
                result, error = None, f"worker process died ({e.__class__.__name__})"
            except Exception as e:
                result, error = None, f"{e.__class__.__name__}: {redact(str(e))}"
            self._finish(task, result, error)
        for lane in broken:
            self._replace_pool(lane)

    def _finish(self, task: Task, result: Optional[Dict], error: Optional[str]):
        logger.debug(f"Finished {task.kind} {task.key}: {error or 'ok'}")
        if task.kind == "item":
            self._busy_feeds.discard(task.key)
            item = Path(task.info["item"])
            if error is None:
                self.spool.done(item, archive=self.config.raw.enabled)
                with self._lock:
                    self._update_status(
                        task.key,
                        **result.get("summary", {}),
                        last_processed_seconds=result.get("seconds"),
                        worker_peak_memory_mb=result.get("peak_memory_mb"),
                    )
            else:
                quarantined = self.spool.release(item, error, self.runtime.max_attempts)
                logger.error(
                    f"{task.key}: processing {item.name} failed ({error})"
                    + ("; moved to quarantine/" if quarantined else "; will retry")
                )
                with self._lock:
                    self._update_status(
                        task.key,
                        last_processing_error=error,
                        quarantined=self.spool.quarantined(),
                    )
        elif task.kind == "static" and error is not None:
            folder = Path(task.info["folder"])
            meta = read_json(folder / "meta.json") or {}
            meta["attempt"] = meta.get("attempt", 1) + 1
            meta["last_error"] = error
            if meta["attempt"] > self.runtime.max_attempts:
                target = self.spool.path(
                    "quarantine", "static", folder.parent.name, folder.name
                )
                target.parent.mkdir(parents=True, exist_ok=True)
                write_json(folder / "meta.json", meta)
                os.replace(folder, target)
                logger.error(
                    f"Static feed {task.key} failed {error}; moved to quarantine/"
                )
            else:
                write_json(folder / "meta.json", meta)
                logger.error(f"Static feed {task.key} failed ({error}); will retry")
        elif error is not None:
            logger.error(f"{task.key} failed: {error}")
        elif result is not None:
            logger.info(
                f"{task.kind} {task.key} done in {result.get('seconds')}s, "
                f"worker peak memory {result.get('peak_memory_mb')} MB"
            )

    def _replace_pool(self, lane: str):
        logger.error(
            f"A {lane} worker process died (out of memory?): replacing the pool"
        )
        old = self._heavy_pool if lane == "heavy" else self._normal_pool
        old.shutdown(wait=False, cancel_futures=True)
        new = self._new_pool(
            self.runtime.heavy_slots if lane == "heavy" else self.runtime.worker_count()
        )
        if lane == "heavy":
            self._heavy_pool = new
        else:
            self._normal_pool = new

    # Windows and raw archive ------------------------------------------------------

    def _close_windows(self):
        now = datetime.now(timezone.utc)
        for window_json in self.spool.path("windows").glob("*/*/*/window.json"):
            folder = window_json.parent
            key = f"window {folder}"
            if key in self._keys_in_flight:
                continue
            window = read_json(window_json)
            if window is None or datetime.fromisoformat(window["end"]) > now:
                continue
            if self._count("normal") >= self.runtime.worker_count() * 2:
                return
            self._submit(
                Task("window", key, "normal"), worker.close_window, str(folder)
            )

    def _bundle_raw(self):
        if not self.config.raw.enabled:
            return
        now = datetime.now(timezone.utc)
        for folder in self.spool.path("raw").glob("*/*/*"):
            feed, day, hour = folder.parent.parent.name, folder.parent.name, folder.name
            key = f"raw {folder}"
            end = datetime.strptime(day + hour, "%Y%m%d%H").replace(tzinfo=timezone.utc)
            if (
                key in self._keys_in_flight
                or (now - end).total_seconds() < 3600 + RAW_GRACE_SECONDS
            ):
                continue
            if feed not in self.feeds:
                continue
            provider, api = self.feeds[feed]
            path = (
                f"{self.config.raw.prefix}/provider={provider.name}/feed={feed_slug(api)}/"
                f"date={day[:4]}-{day[4:6]}-{day[6:]}/{hour}-{int(time.time())}.tar.zst"
            )
            self._submit(
                Task("raw", key, "normal"),
                worker.bundle_raw,
                str(folder),
                provider.name,
                path,
            )

    # Backpressure and status ------------------------------------------------------

    def _check_spool_size(self):
        self._spool_size = self.spool.size_bytes()
        limit = self.runtime.spool_max_gb * 1024**3
        ratio = self._spool_size / limit
        with self._lock:
            if ratio >= 1:
                priorities = sorted({api.priority for _, api in self.feeds.values()})
                # The lowest priority stops first; with a single priority, all do
                cutoff = priorities[0] if len(priorities) == 1 else priorities[-2]
                paused = {
                    f for f, (_, api) in self.feeds.items() if api.priority <= cutoff
                }
                if paused != self._paused:
                    logger.error(
                        f"Spool full ({self._spool_size / 1024**3:.1f} GB): "
                        f"stopped fetching {len(paused)} feeds"
                    )
                self._paused = paused
            elif ratio < 0.9 and self._paused:
                logger.warning("Spool below 90 %: fetching all feeds again")
                self._paused = set()
            if 0.8 <= ratio < 1:
                logger.warning(f"Spool at {ratio:.0%} of spool_max_gb")

    def _update_status(self, feed: str, **fields):
        self._status.setdefault(feed, {}).update(fields)
        self._status_dirty.add(feed)

    def _write_statuses(self, force: bool = False):
        with self._lock:
            dirty = set(self._status) if force else set(self._status_dirty)
            self._status_dirty.clear()
            snapshot = {feed: dict(self._status[feed]) for feed in dirty}
        for feed, status in snapshot.items():
            if feed not in self.feeds:
                continue
            provider, api = self.feeds[feed]
            document = {"url": strip_query(api.url), "services": api.services, **status}
            self.spool.put_ready(
                provider.name,
                f"{provider.name}/_status/{feed_slug(api)}.json",
                _json(document),
            )

    def _write_index(self):
        backlog = self.spool.backlog()
        with self._lock:
            feeds = {
                feed: {
                    "provider": provider.name,
                    "services": api.services,
                    "last_success": self._status.get(feed, {}).get("last_success"),
                    "last_error": self._status.get(feed, {}).get("last_error"),
                    "feed_age_seconds": self._status.get(feed, {}).get(
                        "feed_age_seconds"
                    ),
                    "paused": feed in self._paused,
                    **backlog.get(feed, {"waiting": 0, "oldest_age_seconds": 0}),
                }
                for feed, (provider, api) in self.feeds.items()
            }
        index = {
            "updated": datetime.now(timezone.utc).isoformat(),
            "spool_gb": round(self._spool_size / 1024**3, 3),
            "spool_max_gb": self.runtime.spool_max_gb,
            "quarantined": self.spool.quarantined(),
            "feeds": feeds,
        }
        self.spool.put_ready(GLOBAL, "_status/index.json", _json(index))


def _json(document: Dict) -> bytes:
    import json

    return json.dumps(document, indent=2, default=str).encode("utf-8")


class Uploader(threading.Thread):
    """Uploads spool/ready/<provider>/<path> to the provider's storage at <path>."""

    def __init__(self, spool: Spool, storages, stop: threading.Event):
        super().__init__(name="upload", daemon=True)
        self.spool = spool
        self.storages = storages
        self.stop_event = stop
        self._retry_at: Dict[Path, float] = {}
        self._failures: Dict[Path, int] = {}

    def run(self):
        while not self.stop_event.is_set():
            if not self.upload_some():
                self.stop_event.wait(1)

    def drain(self, timeout: float):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline and self.upload_some(ignore_backoff=True):
            pass
        left = sum(1 for _ in self.spool.ready_files())
        if left:
            logger.warning(
                f"{left} files not uploaded yet; they stay in the spool for the next start"
            )

    def upload_some(self, limit: int = 200, ignore_backoff: bool = False) -> int:
        uploaded = 0
        ready = self.spool.path("ready")
        for path in self.spool.ready_files():
            if uploaded >= limit or (self.stop_event.is_set() and not ignore_backoff):
                break
            if not ignore_backoff and self._retry_at.get(path, 0) > time.monotonic():
                continue
            relative = path.relative_to(ready).parts
            provider, storage_path = relative[0], "/".join(relative[1:])
            storage = self.storages.get(provider, self.storages["global"])
            try:
                storage.save_file(str(path), storage_path)
                if not storage.file_exists(storage_path):
                    raise IOError(f"{storage_path} missing after upload")
                path.unlink(missing_ok=True)
                self._retry_at.pop(path, None)
                self._failures.pop(path, None)
                _remove_empty_parents(path.parent, ready)
                uploaded += 1
            except Exception as e:
                failures = self._failures.get(path, 0) + 1
                self._failures[path] = failures
                self._retry_at[path] = time.monotonic() + min(300, 2**failures)
                logger.warning(
                    f"Upload of {storage_path} failed ({failures}x): {redact(str(e))}"
                )
        return uploaded


def _remove_empty_parents(folder: Path, stop_at: Path):
    while folder != stop_at:
        try:
            folder.rmdir()
        except OSError:
            return
        folder = folder.parent
