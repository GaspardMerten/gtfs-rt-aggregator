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
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..aggregator.service import AggregatorService
from ..config.models import GtfsRtConfig
from ..runtime.core import StaticVersions, process_payload, realtime_feeds
from ..runtime.spool import Spool, read_json, write_atomic, write_json
from ..static.service import StaticService
from ..utils.file_time import format_file_time, parse_file_time
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
        return self.storages.get(provider_name, self.storages["global"])


_CTX: Optional[_Context] = None


def init_worker(config: GtfsRtConfig, spool_root: str, log_level: int):
    """Initializer of a worker process."""
    global _CTX
    # "kill -USR1 <worker pid>" prints what a worker is doing
    import faulthandler
    import signal

    if hasattr(signal, "SIGUSR1"):
        faulthandler.register(signal.SIGUSR1, all_threads=True)
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
    _CTX = _Context(config, spool_root)


def _ctx() -> _Context:
    if _CTX is None:
        raise RuntimeError("init_worker was not called in this process")
    return _CTX


def _timed(task):
    """Add the task's duration and the worker's peak memory to its result."""

    # Same name as the task: the main process sends tasks by name
    @functools.wraps(task)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = task(*args, **kwargs) or {}
        result["seconds"] = round(time.monotonic() - start, 3)
        result["peak_memory_mb"] = round(_peak_memory_mb())
        # Hand memory back to the system between tasks: Arrow's allocator
        # keeps freed memory otherwise (a static feed can leave ~1 GB held)
        gc.collect()
        try:
            pa.default_memory_pool().release_unused()
        except AttributeError:  # older pyarrow
            pass
        return result

    return wrapper


def _peak_memory_mb() -> float:
    from ..utils.scheduler import _peak_memory_mb as peak

    return peak()


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
        for service_type, table in result.tables.items():
            data = ParquetSerializer.pyarrow_table_to_bytes(table, compression="snappy")
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
                path = f"{provider.name}/{service_type}/individual/{format_file_time(fetch_time)}.parquet"
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
    if not (folder / "window.json").exists():
        write_json(
            folder / "window.json",
            {
                "feed": feed,
                "provider": provider_name,
                "service": service_type,
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        )
    part = folder / f"part-{name}.parquet"
    write_atomic(part, data)
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
    # Take the window: a fetch arriving now creates a new folder for itself
    folder = original.with_name(original.name + f".closing-{os.getpid()}")
    try:
        os.replace(original, folder)
    except FileNotFoundError:
        return {"parts": 0}
    window = read_json(folder / "window.json")
    parts = sorted(folder.glob("part-*.parquet"))
    if window is None or not parts:
        shutil.rmtree(folder, ignore_errors=True)
        return {"parts": 0}
    table = pa.concat_tables(
        [pq.read_table(p) for p in parts], promote_options="default"
    )
    first = datetime.strptime(parts[0].stem[len("part-") :], "%Y%m%dT%H%M%S.%fZ")
    path = (
        f"{window['provider']}/{window['service']}/individual/"
        f"{first.strftime('%Y-%m-%d_%H-%M-%S')}Z.parquet"
    )
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
    _ctx().aggregator.compact_once(**kwargs)
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
