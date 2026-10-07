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
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..aggregator.service import AggregatorService
from ..config.models import GtfsRtConfig
from ..runtime.core import (
    StaticVersions,
    feed_hash,
    feed_slug,
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
    # A crash in native code (segfault, abort) prints the Python stacks
    faulthandler.enable(all_threads=True)
    # Ctrl+C reaches the whole process group: the main process stops the
    # workers itself, once running tasks had time to finish. SIGTERM still
    # ends a worker: a broken pool terminates its workers with it, and waits
    # for them while holding its lock (7 Oct 2026: ignoring it froze the
    # pipeline). Run under systemd with KillSignal=SIGINT.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
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
    it would otherwise outlive it, beside the pipeline started next.
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
    """
    Add the task's duration and the worker's peak memory to its result. An
    error comes back as {"error": "<class>: <message>"}, never as an
    exception: one the main process cannot unpickle (minio's S3Error, a class
    defined by an adapter) breaks the whole pool, as if a worker had died.
    """

    # Same name as the task: the main process sends tasks by name
    @functools.wraps(task)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        try:
            result = task(*args, **kwargs) or {}
            result["seconds"] = round(time.monotonic() - start, 3)
            result["peak_memory_mb"] = round(_peak_memory_mb())
            return result
        except Exception as error:
            return {"error": f"{type(error).__name__}: {error}"}
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

    A fetch whose filter needs a static version not readable yet raises
    StaticNotReady: it stays in the spool, however long (see Runtime._finish).
    Unchanged fetches (this one, or those the runtime skipped as identical
    bytes, listed in its sidecar) are recorded in the next file written, or
    in a file with no rows every UNCHANGED_FLUSH_SECONDS (see fetch_times.py).
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
    fetch_times.note_unchanged(state, meta.get("unchanged_fetch_times") or [])
    tables, times = result.tables, fetch_times.of_fetch(feed_hash(api), fetch_time)
    if tables is None:
        fetch_times.note_unchanged(state, [fetch_time.isoformat()])
        if fetch_times.unchanged_flush_due(state, fetch_time):
            # Files with no rows, holding only the unchanged fetches
            tables, times = _empty_tables(provider, api, fetch_time), {}
    written = []
    if tables is not None:
        unchanged = fetch_times.pending_unchanged(state, feed_hash(api))
        if times:
            tables = fetch_times.worth_storing(tables, state)
        for service_type, table in tables.items():
            data = ParquetSerializer.pyarrow_table_to_bytes(
                fetch_times.with_times(table, times, unchanged), compression="snappy"
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
        if written:
            state.pop("unchanged", None)
    if result.tables is not None and ctx.config.output.live_snapshot:
        _write_live(ctx, provider.name, api, result.tables, fetch_time, state)
    # Only once written: after a failure, the same content is tried again
    state["snapshot"] = result.snapshot
    ctx.spool.save_state(feed, state)
    return {"feed": feed, "summary": result.summary, "written": written}


def _empty_tables(provider, api, fetch_time) -> Dict[str, pa.Table]:
    """A table with no rows (but the columns) for each service of a feed."""
    from ..fetcher.gtfs_rt import GtfsRtFetcher, row_metadata

    return GtfsRtFetcher.build_tables(
        [],
        [],
        api.services,
        fetch_time,
        row_metadata(provider.name, fetch_time, None, None, feed_hash(api)),
    )


def _write_live(ctx, provider_name, api, tables, fetch_time, state) -> None:
    """The fetch, whole, as the feed's live snapshot. A failure only costs this snapshot."""
    now = time.time()
    if now - state.get("live_at", 0) < ctx.config.output.live_seconds:
        return
    try:
        for service_type, table in tables.items():
            ctx.storage(provider_name).save_bytes(
                ParquetSerializer.pyarrow_table_to_bytes(table, compression="zstd"),
                f"{provider_name}/_live/{service_type}/{feed_slug(api)}.parquet",
            )
        state["live_at"] = now
    except Exception as e:
        logger.warning(f"Could not write the live snapshot of {api.source}: {e}")


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
    tables, times, unchanged = [], {}, {}
    for part in parts:
        tables.append(pq.read_table(part))
        metadata = tables[-1].schema.metadata
        fetch_times.merge(times, fetch_times.decode(metadata))
        fetch_times.merge(
            unchanged, fetch_times.decode(metadata, fetch_times.UNCHANGED_TIMES_METADATA)
        )
    table = fetch_times.with_times(
        pa.concat_tables(tables, promote_options="default"), times, unchanged
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
def adapter_fetch(
    feed: str,
    previous_sha256: Optional[str],
    unchanged: Optional[List[str]] = None,
    force: bool = False,
) -> Dict:
    """
    One poll of a realtime adapter (see adapters.py): the fetch goes to the
    spool like a download, then its state is saved. Unchanged bytes (with
    skip_unchanged) are not stored, unless force. unchanged: times of the
    feed's earlier unchanged polls, recorded with this fetch (see
    fetch_times.py).
    """
    import hashlib

    from .. import adapters

    ctx = _ctx()
    provider, api = ctx.feeds[feed]
    fetch_time = datetime.now(pytz.timezone(provider.timezone))
    saved = adapters.AdapterState(ctx.spool.path("state", f"{feed}.adapter.json"))
    state = saved.working_copy()
    data = adapters.fetch_realtime(api.adapter, ctx.config.base_dir, state)
    sha256 = hashlib.sha256(data).hexdigest()
    result = {
        "fetch_time": fetch_time.isoformat(),
        "size": len(data),
        "sha256": sha256,
    }
    if api.skip_unchanged and sha256 == previous_sha256 and not force:
        result["unchanged"] = True
    else:
        item = ctx.spool.new_item(feed, fetch_time)
        tmp = item.with_name(item.name + ".part")
        tmp.write_bytes(data)
        meta = {
            "feed": feed,
            "provider": provider.name,
            "url": api.source,
            "services": api.services,
            "fetch_time": fetch_time.isoformat(),
            "size": len(data),
            "sha256": sha256,
            "attempt": 1,
        }
        if unchanged:
            meta["unchanged_fetch_times"] = list(unchanged)
        ctx.spool.commit_item(item, tmp, meta)
        result["item"] = str(item)
    # Only once the fetch is in the spool: after a failure, the adapter is
    # called again with the same state
    saved.save(state)
    return result


@_timed
def static_adapter(provider_name: str, feed_name: str, first: bool) -> Dict:
    """
    Build a static feed with its adapter (see adapters.py) and store it like
    a downloaded zip. At the first check after a start, skipped if a version
    is stored and the adapter ran less than check_minutes ago.
    """
    import tempfile

    from .. import adapters
    from ..static.service import read_latest, static_base

    ctx = _ctx()
    provider = next(p for p in ctx.config.providers if p.name == provider_name)
    static = next(f for f in provider.static if f.name == feed_name)
    saved = adapters.AdapterState(
        ctx.spool.path("state", f"{provider_name}__static__{feed_name}.adapter.json")
    )
    if first and saved.last_run is not None:
        age = (datetime.now(timezone.utc) - saved.last_run).total_seconds()
        if age < static.check_minutes * 60 and read_latest(
            ctx.storage(provider_name), static_base(provider_name, feed_name), logger
        ):
            return {"skipped": True}
    state = saved.working_copy()
    fetch_time = datetime.now(pytz.timezone(provider.timezone))
    with tempfile.TemporaryDirectory(prefix="gtfs_rt_aggregator-static-") as tmp:
        out_dir = Path(tmp, "adapter")
        out_dir.mkdir()
        zip_path = adapters.build_static(
            static.adapter, ctx.config.base_dir, state, out_dir
        )
        ctx.static.process(
            provider_name,
            feed_name,
            str(zip_path),
            {
                "url": f"adapter:{static.adapter}",
                "etag": None,
                "last_modified": None,
                "fetch_time": fetch_time.isoformat(),
            },
            static.reuse_unchanged_tables,
            logger,
        )
    saved.save(state)
    return {}


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
def prune_raw(provider_name: str, prefix: str, retention_days: int) -> Dict:
    """Delete a provider's raw bundles of days (UTC) older than retention_days."""
    import re

    storage = _ctx().storage(provider_name)
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=retention_days)).isoformat()
    deleted = 0
    for path in storage.walk_files(f"{prefix}/provider={provider_name}/"):
        day = re.search(r"/date=(\d{4}-\d{2}-\d{2})/", path)
        if day and day.group(1) < cutoff and storage.delete_file(path) is not False:
            deleted += 1
    if deleted:
        logger.info(f"Raw archive of {provider_name}: deleted {deleted} bundles before {cutoff}")
    return {"deleted": deleted}


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
