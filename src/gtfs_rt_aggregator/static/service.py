import json
import os
import tempfile
import zipfile
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import pytz
import requests

from ..aggregator.service import INDIVIDUAL_TIME_FORMAT
from ..config.models import GtfsRtConfig
from ..storage.base import StorageInterface
from ..utils.log_helper import setup_logger


class StaticService:
    """Service storing a new version of each GTFS static feed when it changes.

    Layout, for a feed named "static" of provider "nl":

        nl/static/2026-09-28_03-00-00+0200/stops.parquet  (one file per table)
        nl/static/2026-09-28_03-00-00+0200/manifest.json
        nl/static/latest.json                             (copy of the last manifest)
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

        if any(provider.static for provider in config.providers):
            try:
                import gtfs_parquet  # noqa: F401
            except ImportError as e:
                raise ImportError(
                    "Static feeds need gtfs-parquet: pip install 'gtfs_rt_aggregator[static]'"
                ) from e

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
                }
                name = f"Static - {provider.name} - {feed.name} - {feed.url}"
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
        url: str,
        timezone: str,
        headers: Optional[Dict[str, str]] = None,
    ):
        """
        Check a static feed once, and store it if it changed.

        @param provider_name: Name of the provider
        @param feed_name: Folder of the feed under the provider folder
        @param url: URL of the GTFS zip
        @param timezone: Timezone of the provider
        @param headers: HTTP headers to send (e.g. an API key)
        """
        logger = setup_logger(f"{__name__}.StaticService.job.{provider_name}")
        base = f"{provider_name}/{feed_name}"

        try:
            storage = self.storages.get(provider_name, self.storages["global"])
            latest = self._read_latest(storage, base)
            fetch_time = datetime.now(pytz.timezone(timezone))

            # Only reuse the cache validators if they belong to the same URL
            request_headers = dict(headers or {})
            if latest and latest.get("url") == url:
                if latest.get("etag"):
                    request_headers["If-None-Match"] = latest["etag"]
                if latest.get("last_modified"):
                    request_headers["If-Modified-Since"] = latest["last_modified"]

            with tempfile.TemporaryDirectory() as tmp:
                zip_path = os.path.join(tmp, "feed.zip")
                with requests.get(
                    url, headers=request_headers, stream=True, timeout=300
                ) as response:
                    if response.status_code == 304:
                        logger.info(f"{base}: not modified since the last check")
                        return
                    response.raise_for_status()
                    with open(zip_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=1 << 20):
                            f.write(chunk)
                    etag = response.headers.get("ETag")
                    last_modified = response.headers.get("Last-Modified")

                files = self._zip_fingerprint(zip_path)
                if not files:
                    raise ValueError(f"No GTFS .txt file found in {url}")

                if latest and latest.get("files") == files:
                    logger.info(f"{base}: unchanged since {latest.get('version')}")
                    # Keep the validators fresh so the next check can get a 304
                    if (etag, last_modified, url) != (
                        latest.get("etag"),
                        latest.get("last_modified"),
                        latest.get("url"),
                    ):
                        latest.update(url=url, etag=etag, last_modified=last_modified)
                        self._save_json(storage, f"{base}/latest.json", latest)
                    return

                tables = self._to_parquet_tables(zip_path, tmp)
                if not tables:
                    raise ValueError(f"No GTFS table could be parsed from {url}")

            version = fetch_time.strftime(INDIVIDUAL_TIME_FORMAT)
            for table_name, data in tables.items():
                storage.save_bytes(data, f"{base}/{version}/{table_name}.parquet")

            manifest = {
                "version": version,
                "fetched_at": fetch_time.isoformat(),
                "url": url,
                "etag": etag,
                "last_modified": last_modified,
                "files": files,
                "tables": sorted(tables),
            }
            self._save_json(storage, f"{base}/{version}/manifest.json", manifest)
            # Written last, so a run that fails halfway is retried at the next
            # check (the incomplete version folder stays behind)
            self._save_json(storage, f"{base}/latest.json", manifest)

            logger.info(f"{base}: stored new version {version} ({len(tables)} tables)")
        except Exception as e:
            logger.error(
                f"Error in static feed job for {base}: {str(e)}", exc_info=True
            )

    def _read_latest(self, storage: StorageInterface, base: str) -> Optional[dict]:
        path = f"{base}/latest.json"
        if not storage.file_exists(path):
            return None
        try:
            return json.loads(storage.read_bytes(path))
        except ValueError:
            # Treated as a first run: the next version is stored in full
            self.logger.warning(f"Could not read {path}, ignoring it")
            return None

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
    def _to_parquet_tables(zip_path: str, tmp: str) -> Dict[str, bytes]:
        """Parse the GTFS zip and return the Parquet bytes of each table."""
        import gtfs_parquet

        feed = gtfs_parquet.parse_gtfs_zip(zip_path)
        if hasattr(gtfs_parquet, "to_parquet_bytes"):
            return gtfs_parquet.to_parquet_bytes(feed)

        # gtfs-parquet < 0.5.0 can only write to disk
        out = os.path.join(tmp, "parquet")
        gtfs_parquet.write_parquet(feed, out)
        tables = {}
        for filename in os.listdir(out):
            with open(os.path.join(out, filename), "rb") as f:
                tables[filename.removesuffix(".parquet")] = f.read()
        return tables
