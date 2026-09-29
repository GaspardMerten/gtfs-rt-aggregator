import html
import importlib.metadata
import itertools
import importlib.util
import json
import re
import os
import tempfile
import zipfile
from datetime import datetime
from urllib.parse import urljoin
from typing import Dict, List, Any, Optional, Tuple

import pytz
import requests

from ..config.models import GtfsRtConfig
from ..storage.base import StorageInterface
from ..utils.file_time import format_file_time
from ..utils.http import get_bytes, raise_for_status, with_retries
from ..utils.log_helper import setup_logger
from ..utils.cleanup import STATIC_WORK_PREFIX
from ..utils.redact import strip_query


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
            if _version_tuple(version) < (0, 6, 1):
                raise ImportError(
                    f"Static feeds need gtfs-parquet 0.6.1 or later, found {version}: "
                    "pip install -U 'gtfs_rt_aggregator[static]'"
                )

    def get_scheduling(self) -> List[Tuple[Any, callable, str, Dict[str, Any]]]:
        """
        Get the scheduling configuration for the static service.

        Returns:
            List of tuples containing (interval in seconds, function, name, arguments)
        """
        schedules = []
        for provider in self.config.providers:
            for feed in provider.static:
                args = {
                    "provider_name": provider.name,
                    "feed_name": feed.name,
                    "url": feed.url,
                    "timezone": provider.timezone,
                    "headers": feed.headers,
                    "index_url": feed.index_url,
                    "url_pattern": feed.url_pattern,
                    "retries": feed.retries,
                    "reuse_unchanged_tables": feed.reuse_unchanged_tables,
                }
                name = f"Static - {provider.name} - {feed.name} - {feed.url or feed.index_url}"
                # Exclusive: two checks of the same feed must not run at once
                schedules.append(
                    (feed.check_minutes * 60, self.run_once, name, args, True)
                )

        self.logger.info(f"Created {len(schedules)} static feed schedules")
        return schedules

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
        storage = self.storages.get(provider_name, self.storages["global"])
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
        storage = self.storages.get(provider_name, self.storages["global"])
        latest = read_latest(storage, base, logger)
        saved_url, etag, last_modified = (
            meta["url"],
            meta["etag"],
            meta["last_modified"],
        )
        fetch_time = datetime.fromisoformat(meta["fetch_time"])

        files = self._zip_fingerprint(zip_path)
        if not files:
            raise ValueError(f"No GTFS .txt file found in {saved_url}")

        if latest and latest.get("files") == files:
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
        converted = self._convert(zip_path, work_dir)
        if not converted:
            raise ValueError(f"No GTFS table could be parsed from {saved_url}")

        version = format_file_time(fetch_time)
        tables = {}
        for table_name, local_path in converted.items():
            source = f"{table_name}.txt"
            if (
                reuse_unchanged_tables
                and latest
                and source in files
                and latest.get("files", {}).get(source) == files[source]
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
        }
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
    def _convert(zip_path: str, out_dir: str) -> Dict[str, str]:
        """Convert the GTFS zip to one Parquet file per table, in out_dir."""
        # Polars' memory grows with its thread count (one per core by default):
        # 4 threads keep a national feed around 0.5 GB. Only effective if Polars
        # was not imported yet in this process, which is the case in a job.
        os.environ.setdefault("POLARS_MAX_THREADS", "4")
        from gtfs_parquet import convert_gtfs_zip

        return convert_gtfs_zip(zip_path, out_dir)
