import gzip
import html
import importlib.metadata
import itertools
import importlib.util
import json
import re
import os
import tempfile
import zipfile
import zlib
from datetime import date, datetime, timedelta
from urllib.parse import urljoin
from typing import Dict, Any, Optional, Tuple

import pytz
import requests

from ..config.models import GtfsRtConfig
from ..storage.base import StorageInterface, storage_for
from ..utils.file_time import format_file_time
from ..utils.http import get_bytes, raise_for_status, with_retries
from ..utils.log_helper import setup_logger
from ..utils.cleanup import STATIC_WORK_PREFIX
from ..utils.redact import strip_query


# min_change compares the trips running in the coming days: a stored version
# is kept while it still describes them
CHANGE_DAYS = 7


def _version_tuple(version: str) -> Tuple[int, ...]:
    """(0, 5, 1) for "0.5.1", "0.5.1.dev3+g1234" or "0.5.1rc1"."""
    parts = []
    for part in version.split(".")[:3]:
        digits = "".join(itertools.takewhile(str.isdigit, part))
        if not digits:
            break
        parts.append(int(digits))
    return tuple(parts)


def static_base(provider_name: str, feed_name: str) -> str:
    """Folder of a static feed's versions and latest.json."""
    return f"{provider_name}/{feed_name}"


def read_latest(storage: StorageInterface, base: str, logger=None) -> Optional[dict]:
    """Manifest of the latest stored version, or None if there is none (or it
    cannot be read: the next check then stores a full version)."""
    path = f"{base}/latest.json"
    if not storage.file_exists(path):
        return None
    try:
        return json.loads(storage.read_bytes(path))
    except ValueError:
        if logger:
            logger.warning(f"Could not read {path}, ignoring it")
        return None


def scrub_urls(storage: StorageInterface, base: str) -> int:
    """
    Remove query strings (which may hold API keys) from the URLs saved in the
    manifests of a static feed, written before 0.5.1. Returns how many files
    were rewritten.
    """
    paths = {
        p
        # Backends differ: some match the pattern on the name, others on the path
        for pattern in ("manifest.json", "*/manifest.json")
        for p in storage.list_files(base, pattern)
        if p.endswith("/manifest.json")
    }
    if storage.file_exists(f"{base}/latest.json"):
        paths.add(f"{base}/latest.json")
    rewritten = 0
    for path in sorted(paths):
        manifest = json.loads(storage.read_bytes(path))
        if manifest.get("url") and strip_query(manifest["url"]) != manifest["url"]:
            manifest["url"] = strip_query(manifest["url"])
            storage.save_bytes(json.dumps(manifest, indent=2).encode("utf-8"), path)
            rewritten += 1
    return rewritten


def _fetch(storage: StorageInterface, path: str, local_path: str):
    with open(local_path, "wb") as f:
        f.write(storage.read_bytes(path))


def manifest_tables(manifest: dict, base: str) -> Dict[str, str]:
    """Storage path of each table of a version (0.3/0.4 manifests list names)."""
    tables = manifest.get("tables", {})
    if isinstance(tables, list):
        return {name: f"{base}/{manifest['version']}/{name}.parquet" for name in tables}
    return tables


class StaticService:
    """Service storing a new version of each GTFS static feed when it changes.

    Layout, for a feed named "static" of provider "nl" (versions named in UTC):

        nl/static/2026-09-28_01-00-00Z/stops.parquet  (one file per table)
        nl/static/2026-09-28_01-00-00Z/manifest.json
        nl/static/latest.json                         (copy of the last manifest)
    """

    def __init__(self, config: GtfsRtConfig, storages: Dict[str, StorageInterface]):
        """
        Initialize the static service.

        @param config: Configuration
        @param storages: Dictionary of storage interfaces by provider name, with 'global' as the default
        """
        self.logger = setup_logger(f"{__name__}.StaticService")
        self.config = config
        self.storages = storages

        # Checked without importing it: Polars reads POLARS_MAX_THREADS when it
        # is first imported, which must happen in the job process (see _convert)
        if any(provider.static for provider in config.providers):
            if importlib.util.find_spec("gtfs_parquet") is None:
                raise ImportError(
                    "Static feeds need gtfs-parquet: pip install 'gtfs_rt_aggregator[static]'"
                )
            version = importlib.metadata.version("gtfs-parquet")
            netex = any(
                static.format == "netex"
                for provider in config.providers
                for static in provider.static
            )
            needed = (0, 7, 0) if netex else (0, 6, 1)
            if _version_tuple(version) < needed:
                raise ImportError(
                    f"{'NeTEx' if netex else 'Static'} feeds need gtfs-parquet "
                    f"{'.'.join(map(str, needed))} or later, found {version}: "
                    "pip install -U 'gtfs_rt_aggregator[static]'"
                )

    def run_once(
        self,
        provider_name: str,
        feed_name: str,
        url: Optional[str],
        timezone: str,
        headers: Optional[Dict[str, str]] = None,
        index_url: Optional[str] = None,
        url_pattern: Optional[str] = None,
        retries: int = 3,
        reuse_unchanged_tables: bool = False,
    ):
        """
        Check a static feed once, and store it if it changed.

        @param provider_name: Name of the provider
        @param feed_name: Folder of the feed under the provider folder
        @param url: URL of the GTFS zip (None when index_url is used)
        @param timezone: Timezone of the provider
        @param headers: HTTP headers to send (e.g. an API key)
        @param index_url: Page listing the zip, for URLs that change
        @param url_pattern: Regular expression matching the zip links on index_url
        @param retries: Retries on connection errors, timeouts and 429/5xx
        @param reuse_unchanged_tables: Point to the previous file of unchanged tables
        """
        logger = setup_logger(f"{__name__}.StaticService.job.{provider_name}")
        base = static_base(provider_name, feed_name)
        try:
            # Named, so that folders left by a killed job can be cleaned up
            with tempfile.TemporaryDirectory(prefix=STATIC_WORK_PREFIX) as tmp:
                meta = self.download(
                    provider_name,
                    feed_name,
                    url,
                    timezone,
                    tmp,
                    headers,
                    index_url,
                    url_pattern,
                    retries,
                    logger,
                )
                if meta is not None:
                    self.process(
                        provider_name,
                        feed_name,
                        os.path.join(tmp, "feed.zip"),
                        meta,
                        reuse_unchanged_tables,
                        logger,
                    )
        except Exception as e:
            logger.error(
                f"Error in static feed job for {base}: {str(e)}", exc_info=True
            )

    def download(
        self,
        provider_name: str,
        feed_name: str,
        url: Optional[str],
        timezone: str,
        dest_dir: str,
        headers: Optional[Dict[str, str]] = None,
        index_url: Optional[str] = None,
        url_pattern: Optional[str] = None,
        retries: int = 3,
        logger=None,
    ) -> Optional[Dict[str, Any]]:
        """
        Download a static feed to dest_dir/feed.zip, unless the server says it
        did not change since the latest stored version.

        @return What process() needs besides the zip (URL without query string,
            ETag, Last-Modified, fetch time), or None if not modified
        @raises Exception: If the download fails after retries
        """
        logger = logger or self.logger
        base = static_base(provider_name, feed_name)
        storage = storage_for(self.storages, provider_name)
        latest = read_latest(storage, base, logger)
        fetch_time = datetime.now(pytz.timezone(timezone))
        if index_url:
            url = self._resolve_url(index_url, url_pattern, headers, retries, logger)

        # Only reuse the cache validators if they belong to the same URL
        request_headers = dict(headers or {})
        # Saved without its query string, which may hold an API key
        saved_url = strip_query(url)
        if latest and strip_query(latest.get("url")) == saved_url:
            if latest.get("etag"):
                request_headers["If-None-Match"] = latest["etag"]
            if latest.get("last_modified"):
                request_headers["If-Modified-Since"] = latest["last_modified"]

        zip_path = os.path.join(dest_dir, "feed.zip")
        validators = with_retries(
            lambda: self._download(url, request_headers, zip_path),
            retries,
            logger,
            f"Downloading {url}",
        )
        if validators is None:
            logger.info(f"{base}: not modified since the last check")
            return None
        etag, last_modified = validators
        return {
            "url": saved_url,
            "etag": etag,
            "last_modified": last_modified,
            "fetch_time": fetch_time.isoformat(),
        }

    def process(
        self,
        provider_name: str,
        feed_name: str,
        zip_path: str,
        meta: Dict[str, Any],
        reuse_unchanged_tables: bool = False,
        logger=None,
    ):
        """
        Store a downloaded static feed as a new version, if a file in the zip
        changed since the latest stored version.

        @param zip_path: The downloaded zip (its folder is used for temporary files)
        @param meta: What download() returned
        @raises Exception: If the feed cannot be read or stored
        """
        logger = logger or self.logger
        base = static_base(provider_name, feed_name)
        storage = storage_for(self.storages, provider_name)
        latest = read_latest(storage, base, logger)
        saved_url, etag, last_modified = (
            meta["url"],
            meta["etag"],
            meta["last_modified"],
        )
        fetch_time = datetime.fromisoformat(meta["fetch_time"])

        static = self._static_config(provider_name, feed_name)
        netex = static is not None and static.format == "netex"
        files = (
            self._netex_fingerprint(zip_path) if netex else self._zip_fingerprint(zip_path)
        )
        if not files:
            kind = "NeTEx .xml" if netex else "GTFS .txt"
            raise ValueError(f"No {kind} file found in {saved_url}")

        route_types = sorted(static.route_type_set()) if static else []
        expired = bool(
            latest
            and static
            and static.max_days
            and latest.get("fetched_at")
            and (fetch_time - datetime.fromisoformat(latest["fetched_at"])).total_seconds()
            >= static.max_days * 86400
        )
        # checked_files: a later check found changes too small to store
        # (min_change), which max_days eventually stores
        # A new route_types stores the feed again, filtered the new way
        if latest and latest.get("route_types", []) == route_types and (
            files == latest.get("files")
            or (files == latest.get("checked_files") and not expired)
        ):
            logger.info(f"{base}: unchanged since {latest.get('version')}")
            # Keep the validators fresh so the next check can get a 304
            if (etag, last_modified, saved_url) != (
                latest.get("etag"),
                latest.get("last_modified"),
                latest.get("url"),
            ):
                latest.update(url=saved_url, etag=etag, last_modified=last_modified)
                self._save_json(storage, f"{base}/latest.json", latest)
            return

        work_dir = os.path.join(os.path.dirname(zip_path), "parquet")
        converted = self._convert(zip_path, work_dir, netex)
        if not converted:
            raise ValueError(f"No GTFS table could be parsed from {saved_url}")

        filtered = (
            self._keep_route_types(converted, route_types, saved_url)
            if route_types
            else set()
        )
        signatures = None
        if static and static.min_change:
            signatures = os.path.join(os.path.dirname(zip_path), "trip_signatures.parquet")
            if not self._trip_signatures(converted, signatures):
                signatures = None

        same_filter = latest is not None and latest.get("route_types", []) == route_types
        if signatures and same_filter:
            share = self._changed_share(
                storage, base, latest, converted, signatures, fetch_time.date(), logger
            )
            if share is not None and share < static.min_change and not expired:
                logger.info(
                    f"{base}: {share:.1%} of trips changed since {latest.get('version')}, "
                    f"below min_change {static.min_change:.1%}: not stored"
                )
                # Not "files": reuse_unchanged_tables compares with what is stored
                latest.update(
                    url=saved_url,
                    etag=etag,
                    last_modified=last_modified,
                    checked_files=files,
                    checked_at=fetch_time.isoformat(),
                    checked_change=round(share, 4),
                )
                self._save_json(storage, f"{base}/latest.json", latest)
                return

        def unchanged(source: str) -> bool:
            return source in files and latest.get("files", {}).get(source) == files[source]

        version = format_file_time(fetch_time)
        tables = {}
        for table_name, local_path in converted.items():
            source = f"{table_name}.txt"
            if (
                reuse_unchanged_tables
                and same_filter
                and unchanged(source)
                # A filtered table also depends on which routes and trips are kept
                and (
                    table_name not in filtered
                    or (unchanged("routes.txt") and unchanged("trips.txt"))
                )
                and table_name in manifest_tables(latest, base)
            ):
                tables[table_name] = manifest_tables(latest, base)[table_name]
                continue
            path = f"{base}/{version}/{table_name}.parquet"
            storage.save_file(str(local_path), path)
            tables[table_name] = path

        manifest = {
            "version": version,
            "fetched_at": fetch_time.isoformat(),
            "url": saved_url,
            "etag": etag,
            "last_modified": last_modified,
            "files": files,
            # Storage path of each table; with reuse_unchanged_tables, some
            # point to an earlier version's folder
            "tables": tables,
            "route_types": route_types,
        }
        if signatures:
            # Compared with the next check's, for min_change
            path = f"{base}/{version}/_trip_signatures.parquet"
            storage.save_file(signatures, path)
            manifest["signatures"] = path
            manifest["signature_version"] = self._signature_version()
        self._save_json(storage, f"{base}/{version}/manifest.json", manifest)
        # Written last, so a run that fails halfway is retried at the next
        # check (the incomplete version folder stays behind)
        self._save_json(storage, f"{base}/latest.json", manifest)

        reused = sum(not p.startswith(f"{base}/{version}/") for p in tables.values())
        logger.info(
            f"{base}: stored new version {version} ({len(tables)} tables, {reused} reused)"
        )

    @staticmethod
    def _download(
        url: str, headers: Dict[str, str], zip_path: str
    ) -> Optional[Tuple[Optional[str], Optional[str]]]:
        """Download url to zip_path; return (ETag, Last-Modified), or None on 304."""
        with requests.get(url, headers=headers, stream=True, timeout=300) as response:
            if response.status_code == 304:
                return None
            raise_for_status(response)
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
            return response.headers.get("ETag"), response.headers.get("Last-Modified")

    @staticmethod
    def _resolve_url(
        index_url: str,
        url_pattern: str,
        headers: Optional[Dict[str, str]],
        retries: int,
        logger,
    ) -> str:
        """
        Find the current zip URL on index_url: the greatest link matching
        url_pattern, so that dated URLs (e.g. gtfs-20260928.zip) give the newest.
        """
        # Unescaped, so that links written with &amp; in HTML match
        page = html.unescape(
            get_bytes(index_url, headers, retries, logger).decode("utf-8", "replace")
        )
        matches = {
            urljoin(index_url, m.group(0)) for m in re.finditer(url_pattern, page)
        }
        if not matches:
            raise ValueError(f"No link matching {url_pattern!r} on {index_url}")
        url = max(matches)
        logger.info(f"Resolved {index_url} to {url}")
        return url

    @staticmethod
    def _save_json(storage: StorageInterface, path: str, data: dict):
        storage.save_bytes(json.dumps(data, indent=2).encode("utf-8"), path)

    @staticmethod
    def _zip_fingerprint(zip_path: str) -> Dict[str, Dict[str, int]]:
        """
        Checksum and size of each GTFS file in the zip, read from the zip index.

        Agencies often rebuild the zip with new timestamps while the files stay
        the same: comparing the files, not the zip, ignores that.
        """
        with zipfile.ZipFile(zip_path) as zf:
            return {
                info.filename: {"crc": info.CRC, "size": info.file_size}
                for info in zf.infolist()
                # GTFS files must be at the root of the zip
                if "/" not in info.filename and info.filename.endswith(".txt")
            }

    @staticmethod
    def _netex_fingerprint(path: str) -> Dict[str, Dict[str, int]]:
        """
        Checksum and size of each NeTEx XML document in the download.

        A gzipped file is checksummed uncompressed, since its header holds the
        time it was compressed; a zip, from its index.
        """
        with open(path, "rb") as f:
            magic = f.read(4)
        if magic == b"PK\x03\x04":
            with zipfile.ZipFile(path) as zf:
                return {
                    info.filename: {"crc": info.CRC, "size": info.file_size}
                    for info in zf.infolist()
                    if info.filename.lower().endswith(".xml")
                }
        opener = gzip.open if magic[:2] == b"\x1f\x8b" else open
        crc, size = 0, 0
        with opener(path, "rb") as f:
            while chunk := f.read(1 << 20):
                crc = zlib.crc32(chunk, crc)
                size += len(chunk)
        return {"netex.xml": {"crc": crc, "size": size}} if size else {}

    def _static_config(self, provider_name: str, feed_name: str):
        provider = next(
            (p for p in self.config.providers if p.name == provider_name), None
        )
        return next(
            (f for f in (provider.static if provider else []) if f.name == feed_name),
            None,
        )

    @staticmethod
    def _keep_route_types(converted: Dict[str, str], route_types, url) -> set:
        """
        Keep only the routes of route_types in the converted tables, and the
        trips, stop times, frequencies, services and shapes they use. Stops are
        all kept. Returns the names of the tables that were filtered.
        """
        import polars as pl

        if "routes" not in converted or "trips" not in converted:
            raise ValueError(f"route_types needs routes and trips, missing in {url}")

        def rewrite(name, frame):
            part = f"{converted[name]}.part"
            frame.write_parquet(part, compression="zstd", compression_level=9)
            os.replace(part, converted[name])

        routes = pl.read_parquet(converted["routes"])
        routes = routes.filter(pl.col("route_type").cast(pl.Int32).is_in(route_types))
        if routes.is_empty():
            raise ValueError(f"No route of route_types {route_types} in {url}")
        rewrite("routes", routes)
        trips = pl.read_parquet(converted["trips"]).filter(
            pl.col("route_id").is_in(routes["route_id"].implode())
        )
        rewrite("trips", trips)
        keys = {
            "stop_times": "trip_id",
            "frequencies": "trip_id",
            "calendar": "service_id",
            "calendar_dates": "service_id",
            "shapes": "shape_id",
        }
        filtered = {"routes", "trips"}
        for name, key in keys.items():
            if name not in converted or key not in trips.columns:
                continue
            kept = trips[key].drop_nulls().unique().implode()
            # Streamed: stop_times can be millions of rows
            frame = pl.scan_parquet(converted[name]).filter(pl.col(key).is_in(kept))
            part = f"{converted[name]}.part"
            # As gtfs-parquet writes them
            frame.sink_parquet(part, compression="zstd", compression_level=9)
            os.replace(part, converted[name])
            filtered.add(name)
        return filtered

    @staticmethod
    def _signature_version() -> str:
        # Polars' hashes may differ between its versions
        import polars as pl

        return f"polars-{pl.__version__}"

    @staticmethod
    def _trip_signatures(tables: Dict[str, str], out_path: str) -> bool:
        """
        Write one row per trip: trip_id, route_id, service_id and a hash of its
        stop times. False if the feed has no trips or stop times.
        """
        import polars as pl

        if "trips" not in tables or "stop_times" not in tables:
            return False
        st = pl.scan_parquet(tables["stop_times"])
        columns = [
            c
            for c in ("stop_sequence", "stop_id", "arrival_time", "departure_time")
            if c in st.collect_schema().names()
        ]
        # Summed: the hash of a trip's stop times does not depend on row order
        # (stop_sequence is in each row's hash). Divided so a long trip cannot overflow.
        times = st.group_by("trip_id").agg(
            (pl.struct(columns).hash(seed=0) // 65536).sum().alias("stops")
        )
        trips = pl.scan_parquet(tables["trips"]).select("trip_id", "route_id", "service_id")
        trips.join(times, on="trip_id", how="left").sink_parquet(out_path)
        return True

    @staticmethod
    def _service_days(tables: Dict[str, str], first: date):
        """
        service_id and a bit mask of the days it runs among the CHANGE_DAYS
        days from first (bit i: first + i days), from calendar and
        calendar_dates. None if the feed has neither.
        """
        import polars as pl

        days = pl.DataFrame(
            {
                "i": range(CHANGE_DAYS),
                "date": [first + timedelta(days=i) for i in range(CHANGE_DAYS)],
            }
        ).with_columns(weekday=pl.col("date").dt.weekday())
        weekdays = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        parts = []
        if "calendar" in tables:
            calendar = pl.read_parquet(tables["calendar"])
            runs = calendar.join(days, how="cross").filter(
                pl.col("date").is_between(pl.col("start_date"), pl.col("end_date"))
                & (
                    pl.concat_list(weekdays)
                    .list.get(pl.col("weekday").cast(pl.Int64) - 1)
                    .cast(pl.Int32)
                    == 1
                )
            )
            parts.append(runs.select("service_id", "i", pl.lit(1, pl.Int8).alias("kind")))
        if "calendar_dates" in tables:
            exceptions = pl.read_parquet(tables["calendar_dates"]).join(days, on="date")
            parts.append(
                exceptions.select(
                    "service_id", "i", pl.col("exception_type").cast(pl.Int8).alias("kind")
                )
            )
        if not parts:
            return None
        # A removed day (exception_type 2) wins over the calendar and an added day
        runs = (
            pl.concat(parts)
            .group_by("service_id", "i")
            .agg((pl.col("kind") == 2).any().alias("removed"))
            .filter(~pl.col("removed"))
        )
        return runs.group_by("service_id").agg(
            (pl.lit(1, pl.UInt64) * 2 ** pl.col("i").cast(pl.UInt64)).sum().alias("days")
        )

    def _changed_share(
        self,
        storage: StorageInterface,
        base: str,
        latest: dict,
        tables: Dict[str, str],
        new_path: str,
        first: date,
        logger,
    ) -> Optional[float]:
        """
        Share of the trips running in the CHANGE_DAYS days from first that
        were added, removed or changed (route, days or stop times) since the
        latest stored version, or None if its trips cannot be read. Feeds that
        renumber their services, or put the date in their trip ids, only
        change with their timetable. The latest version's signatures are
        computed and saved if it has none (stored before 0.7.8, or with another
        Polars).
        """
        import polars as pl

        folder = os.path.dirname(new_path)
        stored = manifest_tables(latest, base)
        if "trips" not in stored or "stop_times" not in stored:
            return None
        old = {}
        for name in ("calendar", "calendar_dates"):
            if name in stored:
                old[name] = os.path.join(folder, f"latest_{name}.parquet")
                _fetch(storage, stored[name], old[name])
        old_path = os.path.join(folder, "latest_signatures.parquet")
        if (
            latest.get("signatures")
            and latest.get("signature_version") == self._signature_version()
            and storage.file_exists(latest["signatures"])
        ):
            _fetch(storage, latest["signatures"], old_path)
        else:
            local = {}
            for name in ("trips", "stop_times"):
                local[name] = os.path.join(folder, f"latest_{name}.parquet")
                _fetch(storage, stored[name], local[name])
            self._trip_signatures(local, old_path)
            for path in local.values():
                os.remove(path)
            path = f"{base}/{latest['version']}/_trip_signatures.parquet"
            storage.save_file(old_path, path)
            latest.update(signatures=path, signature_version=self._signature_version())
            self._save_json(storage, f"{base}/latest.json", latest)
            logger.info(f"{base}: computed the signatures of {latest['version']}")

        def running(path, calendars):
            trips = pl.read_parquet(path)
            days = self._service_days(calendars, first)
            if days is None:
                # No calendar: every trip counts
                return trips.with_columns(days=pl.lit(1, pl.UInt64)).drop("service_id")
            return trips.join(days, on="service_id", how="inner").drop("service_id")

        before = running(old_path, old).rename(
            {"route_id": "old_route", "stops": "old_stops", "days": "old_days"}
        )
        after = running(new_path, tables)
        both = before.join(after, on="trip_id", how="full", coalesce=True)
        if both.is_empty():
            return 0.0
        changed = ~(
            pl.col("old_route").eq_missing(pl.col("route_id"))
            & pl.col("old_stops").eq_missing(pl.col("stops"))
            & pl.col("old_days").eq_missing(pl.col("days"))
        )
        return both.select(changed.mean()).item()

    @staticmethod
    def _convert(zip_path: str, out_dir: str, netex: bool = False) -> Dict[str, str]:
        """Convert the GTFS zip (or NeTEx file) to one Parquet file per table, in out_dir."""
        # Polars' memory grows with its thread count (one per core by default):
        # 4 threads keep a national feed around 0.5 GB. Only effective if Polars
        # was not imported yet in this process, which is the case in a job.
        os.environ.setdefault("POLARS_MAX_THREADS", "4")
        if netex:
            from gtfs_parquet import convert_netex

            return convert_netex(zip_path, out_dir)
        from gtfs_parquet import convert_gtfs_zip

        return convert_gtfs_zip(zip_path, out_dir)
