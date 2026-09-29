"""
Tasks run by the worker processes. Each worker builds its own storages and
services once (init_worker), then runs tasks sent by the main process. Tasks
take and return only plain values, so they work with any start method.
"""

import functools
import gc
import logging
import os
import shutil
import tarfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..aggregator.service import AggregatorService
from ..config.models import GtfsRtConfig
from ..runtime.core import (
    StaticVersions,
    feed_hash,
    process_payload,
    realtime_feeds,
)
from ..storage.base import storage_for
from ..runtime.spool import Spool, read_json, write_atomic, write_json
from ..static.service import StaticService
from ..utils.file_time import format_file_time
from ..aggregator import fetch_times
from ..utils.serializer import ParquetSerializer

logger = logging.getLogger(__name__)


class _Context:
    def __init__(self, config: GtfsRtConfig, spool_root: str):
        from ..pipeline import create_storages

        self.config = config
        self.spool = Spool(spool_root)
        self.storages = create_storages(config)
        self.feeds = realtime_feeds(config)
        self.static_versions = StaticVersions(self.storages, logger)
        self.aggregator = AggregatorService(config, self.storages)
        self._static = None

    @property
    def static(self) -> StaticService:
        # Created on first use: it checks that gtfs-parquet is installed
        if self._static is None:
            self._static = StaticService(self.config, self.storages)
        return self._static

    def storage(self, provider_name: str):
        return storage_for(self.storages, provider_name)


_CTX: Optional[_Context] = None
# Arguments of init_worker, to build the context on first use
_INIT: Optional[tuple] = None


def init_worker(
    config: GtfsRtConfig,
    spool_root: str,
    log_level: int,
    main_pid: Optional[int] = None,
):
    """Initializer of a worker process."""
    global _CTX, _INIT
    # "kill -USR1 <worker pid>" prints what a worker is doing
    import faulthandler
    import signal

    if hasattr(signal, "SIGUSR1"):
        faulthandler.register(signal.SIGUSR1, all_threads=True)
    # Ctrl+C and SIGTERM reach the whole process group: the main process
    # stops the workers itself, once running tasks had time to finish
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if main_pid is not None:
        _exit_with(main_pid)
    root = logging.getLogger()
    if not root.handlers:
        # Workers started by a fork server do not inherit the logging setup
        logging.basicConfig(
            level=log_level,
            format="%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s",
        )
    root.setLevel(log_level)
    from ..utils.redact import install_redaction

    install_redaction()
    # Polars' memory grows with its thread count: see StaticService._convert
    os.environ.setdefault("POLARS_MAX_THREADS", "4")
    _CTX, _INIT = None, (config, spool_root)


def _exit_with(main_pid: int, every_seconds: float = 2.0):
    """
    End this worker when the main process is gone (killed, out of memory):
    it ignores SIGTERM, so it would otherwise outlive it, beside the
    pipeline started next.
    """
    import threading

    def watch():
        while True:
            time.sleep(every_seconds)
            try:
                os.kill(main_pid, 0)
            except (ProcessLookupError, PermissionError):
                # PermissionError: the pid now belongs to another user
                os._exit(1)

    threading.Thread(target=watch, name="main-watch", daemon=True).start()


def _ctx() -> _Context:
    """
    The worker's context, built by its first task: an error (e.g. a storage
    that cannot be reached) fails that task, which is retried, instead of
    breaking the whole pool.
    """
    global _CTX
    if _CTX is None:
        if _INIT is None:
            raise RuntimeError("init_worker was not called in this process")
        _CTX = _Context(*_INIT)
    return _CTX


def _timed(task):
    """Add the task's duration and the worker's peak memory to its result."""

    # Same name as the task: the main process sends tasks by name
    @functools.wraps(task)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        try:
            result = task(*args, **kwargs) or {}
            result["seconds"] = round(time.monotonic() - start, 3)
            result["peak_memory_mb"] = round(_peak_memory_mb())
            return result
        finally:
            # Hand memory back to the system between tasks, failed ones too:
            # Arrow's allocator keeps freed memory otherwise (a static feed
            # can leave ~1 GB held)
            gc.collect()
            try:
                pa.default_memory_pool().release_unused()
            except AttributeError:  # older pyarrow
                pass

    return wrapper


def _peak_memory_mb() -> float:
    """Peak resident memory of this process, in MB."""
    import resource
    import sys

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # Kilobytes on Linux, bytes on macOS
    return peak / (1024 * 1024 if sys.platform == "darwin" else 1024)


# Realtime fetches -------------------------------------------------------------


@_timed
def process_item(item_path: str) -> Dict:
    """
    Process one fetch from processing/: parse, skip if unchanged, filter,
    write Parquet (to ready/, or to its accumulate_minutes window).
    """
    ctx = _ctx()
    item = Path(item_path)
    meta = ctx.spool.meta(item)
    feed = meta["feed"]
    provider, api = ctx.feeds[feed]
    tz = pytz.timezone(provider.timezone)
    fetch_time = datetime.fromisoformat(meta["fetch_time"]).astimezone(tz)
    state = ctx.spool.state(feed)

    result = process_payload(
        item.read_bytes(),
        fetch_time,
        provider,
        api,
        ctx.storage(provider.name),
        ctx.static_versions,
        state.get("snapshot"),
        logger,
    )
    written = []
    if result.tables is not None:
        times = fetch_times.of_fetch(feed_hash(api), fetch_time)
        tables = fetch_times.worth_storing(result.tables, state)
        for service_type, table in tables.items():
            data = ParquetSerializer.pyarrow_table_to_bytes(
                fetch_times.with_times(table, times), compression="snappy"
            )
            if api.accumulate_minutes:
                written.append(
                    _add_to_window(
                        ctx,
                        feed,
                        provider.name,
                        service_type,
                        api,
                        fetch_time,
                        item.stem,
                        data,
                    )
                )
            else:
                name = format_file_time(fetch_time, feed_hash(api))
                path = f"{provider.name}/{service_type}/individual/{name}.parquet"
                ctx.spool.put_ready(provider.name, path, data)
                written.append(path)
    # Only once written: after a failure, the same content is tried again
    state["snapshot"] = result.snapshot
    ctx.spool.save_state(feed, state)
    return {"feed": feed, "summary": result.summary, "written": written}


def _add_to_window(
    ctx, feed, provider_name, service_type, api, fetch_time, name, data
) -> str:
    """Add a fetch to its clock-aligned accumulate_minutes window."""
    start = AggregatorService._get_rounded_time(fetch_time, api.accumulate_minutes)
    end = AggregatorService._localize(
        start.replace(tzinfo=None) + timedelta(minutes=api.accumulate_minutes), start
    )
    folder = ctx.spool.path("windows", feed, service_type, format_file_time(start))
    part = folder / f"part-{name}.parquet"
    write_atomic(part, data)
    # After the part: if the window was being closed meanwhile (its folder
    # renamed), the part is in a new folder, which needs its window.json too
    if not (folder / "window.json").exists():
        write_json(
            folder / "window.json",
            {
                "feed": feed,
                "provider": provider_name,
                "service": service_type,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "feed_hash": feed_hash(api),
            },
        )
    return str(part)


@_timed
def close_window(folder: str) -> Dict:
    """
    Concatenate the parts of a finished window into one file in ready/. The
    file is named after the first fetch, like an individual file. Parts added
    while closing (a slow fetch) stay for the next close.
    """
    ctx = _ctx()
    original = Path(folder)
    # Take the window: a fetch arriving now creates a new folder for itself.
    # A window whose closing failed before keeps one .closing- suffix.
    base = original.name.split(".closing-")[0]
    folder = original.with_name(base + f".closing-{os.getpid()}")
    try:
        os.replace(original, folder)
    except FileNotFoundError:
        return {"parts": 0}
    window = read_json(folder / "window.json")
    parts = sorted(folder.glob("part-*.parquet"))
    if window is None or not parts:
        shutil.rmtree(folder, ignore_errors=True)
        return {"parts": 0}
    tables, times = [], {}
    for part in parts:
        tables.append(pq.read_table(part))
        fetch_times.merge(times, fetch_times.decode(tables[-1].schema.metadata))
    table = fetch_times.with_times(
        pa.concat_tables(tables, promote_options="default"), times
    )
    first = datetime.strptime(
        parts[0].stem[len("part-") :], "%Y%m%dT%H%M%S.%fZ"
    ).replace(tzinfo=timezone.utc)
    # Windows opened before 0.7.3 have no feed_hash
    name = format_file_time(first, window.get("feed_hash"))
    path = f"{window['provider']}/{window['service']}/individual/{name}.parquet"
    ctx.spool.put_ready(
        window["provider"],
        path,
        ParquetSerializer.pyarrow_table_to_bytes(table, compression="snappy"),
    )
    shutil.rmtree(folder, ignore_errors=True)
    return {"parts": len(parts), "rows": table.num_rows, "path": path}


# Heavy tasks -------------------------------------------------------------------


@_timed
def process_static(
    provider_name: str, feed_name: str, folder: str, reuse_unchanged_tables: bool
) -> Dict:
    """Store a downloaded static feed (spool/static/<feed>/<time>/) if it changed."""
    ctx = _ctx()
    folder = Path(folder)
    meta = read_json(folder / "meta.json")
    ctx.static.process(
        provider_name,
        feed_name,
        str(folder / "feed.zip"),
        meta,
        reuse_unchanged_tables,
        logger,
    )
    shutil.rmtree(folder, ignore_errors=True)
    return {}


@_timed
def aggregate(**kwargs) -> Dict:
    _ctx().aggregator.run_once(**kwargs)
    return {}


@_timed
def compact(**kwargs) -> Dict:
    return {"days": _ctx().aggregator.compact_once(**kwargs)}


@_timed
def trip_stop_events(provider_name: str, days_back: int = 7) -> Dict:
    from ..aggregator.trip_stop_service import TripStopEventsService

    service = TripStopEventsService(_ctx().config, _ctx().storages)
    # One day per task: the lane stays free for other heavy tasks in between
    days = service.run_once(provider_name, days_back, max_days=1)
    return {"days": [day.isoformat() for day in days]}


@_timed
def iceberg_sync(days_back: Optional[int] = 7) -> Dict:
    from ..sinks.iceberg import IcebergSink

    return {"registered": IcebergSink(_ctx().config, _ctx().storages).sync(days_back)}


@_timed
def iceberg_maintain() -> Dict:
    from ..sinks.iceberg import IcebergSink

    IcebergSink(_ctx().config, _ctx().storages).maintain()
    return {}


@_timed
def bundle_raw(folder: str, provider_name: str, storage_path: str) -> Dict:
    """
    Bundle the raw fetches of one feed and hour (.pb + .json) into a
    zstd-compressed tar in ready/, then delete them.
    """
    ctx = _ctx()
    folder = Path(folder)
    files = sorted(p for p in folder.iterdir() if p.is_file())
    if not files:
        shutil.rmtree(folder, ignore_errors=True)
        return {"files": 0}
    target = ctx.spool.ready_path(provider_name, storage_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + f".tmp-{os.getpid()}")
    with pa.CompressedOutputStream(str(tmp), "zstd") as stream:
        with tarfile.open(fileobj=stream, mode="w|") as tar:
            for path in files:
                tar.add(str(path), arcname=path.name)
    os.replace(tmp, target)
    # Only what was bundled: a late fetch moved in meanwhile waits for the next bundle
    for path in files:
        path.unlink(missing_ok=True)
    try:
        folder.rmdir()
    except OSError:
        pass
    return {"files": len(files), "path": storage_path}
