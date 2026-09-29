"""
Daily TripStopEvent files: for each finished service date D, the TripUpdate
files of the local days D-1, D and D+1 and the static versions they name are
downloaded, and build_trip_stop_events writes

    <folder of path_template(service="TripStopEvent", start=D)>/<compacted_name>

A day is built when D+1 is over and aggregated (trips of D run up to about
33:00, updates come up to WINDOW_MAX after a run's start), and built again
when its TripUpdate files change (late files, the compaction of D+1): the
names and sizes of the files each day was built from are kept in
<provider>/_state/trip_stop_events.json.
"""

import hashlib
import json
import logging
import os
import posixpath
import tempfile
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..config.models import ApiConfig, GtfsRtConfig, ProviderConfig
from ..static.service import manifest_tables, read_latest, static_base
from ..storage.base import StorageInterface, storage_for
from .paths import day_files, day_folder

logger = logging.getLogger(__name__)

SERVICE = "TripStopEvent"
# After the end of D+1, the time for its last files to be aggregated (plus
# the feed's aggregation period)
READY_GRACE = timedelta(minutes=15)
STATIC_TABLES = ("trips", "calendar", "calendar_dates", "stop_times")


def trip_update_feed(provider: ProviderConfig) -> Optional[ApiConfig]:
    """The provider's realtime feed with trip updates, if any."""
    return next(
        (api for api in provider.realtime if "TripUpdate" in api.services), None
    )


def _plain_types(table: pa.Table) -> pa.Table:
    """Polars writes large strings; the other services use plain strings."""
    schema = pa.schema(
        [
            (
                field.with_type(pa.string())
                if pa.types.is_large_string(field.type)
                or pa.types.is_string_view(field.type)
                else field
            )
            for field in table.schema
        ]
    )
    return table.cast(schema)


class TripStopEventsService:
    def __init__(self, config: GtfsRtConfig, storages: Dict[str, StorageInterface]):
        self.config = config
        self.storages = storages

    def _storage(self, provider_name: str) -> StorageInterface:
        return storage_for(self.storages, provider_name)

    def output_path(self, provider: str, day: date, tz) -> str:
        return posixpath.join(
            day_folder(self.config, provider, SERVICE, day, tz),
            self.config.output.compacted_name,
        )

    def run_once(
        self,
        provider_name: str,
        days_back: int = 7,
        now: Optional[datetime] = None,
        max_days: Optional[int] = None,
    ) -> List[date]:
        """
        Build the missing days among the last days_back ready ones (at most
        max_days, oldest first). Returns the days written.
        """
        provider = next(p for p in self.config.providers if p.name == provider_name)
        api = trip_update_feed(provider)
        static = provider.static_for(api) if api else None
        if api is None or static is None:
            logger.warning(
                f"{SERVICE}: {provider_name} needs a TripUpdate feed and its static feed"
            )
            return []
        tz = pytz.timezone(provider.timezone)
        now = (now or datetime.now(pytz.utc)).astimezone(tz)
        period = timedelta(minutes=api.frequency_minutes)
        storage = self._storage(provider_name)
        state = self._read_state(storage, provider_name)
        listings: Dict[date, List[str]] = {}
        written = []
        for back in range(days_back, 1, -1):
            if max_days is not None and len(written) >= max_days:
                break
            day = now.date() - timedelta(days=back)
            end = day + timedelta(days=2)  # end of D+1
            ready = (
                tz.localize(datetime(end.year, end.month, end.day))
                + period
                + READY_GRACE
            )
            if now < ready:
                continue
            try:
                inputs = self._signature(storage, provider_name, day, tz, listings)
                if storage.file_exists(self.output_path(provider_name, day, tz)):
                    known = state.get(day.isoformat())
                    if known is None:
                        # Built before 0.7.4: taken as up to date
                        state[day.isoformat()] = inputs
                        self._write_state(storage, provider_name, state, now, days_back)
                    if known in (None, inputs):
                        continue
                    logger.info(
                        f"{SERVICE}: {provider_name} {day} changed, building it again"
                    )
                if self.build_day(provider, static.name, day, tz):
                    written.append(day)
                    state[day.isoformat()] = inputs
                    self._write_state(storage, provider_name, state, now, days_back)
            except Exception:
                logger.exception(f"{SERVICE}: failed for {provider_name} {day}")
        return written

    def _signature(self, storage, provider: str, day: date, tz, listings) -> str:
        """Names and sizes of the TripUpdate files of D-1, D and D+1, hashed."""
        entries = []
        for offset in (-1, 0, 1):
            other = day + timedelta(days=offset)
            if other not in listings:
                listings[other] = [
                    f"{posixpath.basename(path)}:{storage.file_size(path)}"
                    for path in self._day_files(storage, provider, other, tz)
                ]
            entries += [f"{other}/{entry}" for entry in listings[other]]
        return hashlib.sha1("\n".join(entries).encode()).hexdigest()

    @staticmethod
    def _state_path(provider: str) -> str:
        return f"{provider}/_state/trip_stop_events.json"

    def _read_state(self, storage, provider: str) -> Dict[str, str]:
        path = self._state_path(provider)
        try:
            if storage.file_exists(path):
                return json.loads(storage.read_bytes(path))
        except (OSError, ValueError) as e:
            logger.warning(f"{SERVICE}: could not read {path} ({e})")
        return {}

    def _write_state(self, storage, provider, state, now, days_back):
        oldest = (now.date() - timedelta(days=days_back + 3)).isoformat()
        for day in [d for d in state if d < oldest]:
            del state[day]
        storage.save_bytes(
            json.dumps(state, indent=2, sort_keys=True).encode(),
            self._state_path(provider),
        )

    def _day_files(self, storage, provider: str, day: date, tz) -> List[str]:
        return day_files(
            storage, day_folder(self.config, provider, "TripUpdate", day, tz)
        )

    def build_day(
        self, provider: ProviderConfig, static_name: str, day: date, tz
    ) -> bool:
        """Build and store one service date. False if there was nothing to build."""
        from .trip_stop_events import StaticTimetable, build_trip_stop_events

        storage = self._storage(provider.name)
        if not self._day_files(storage, provider.name, day, tz):
            return False
        with tempfile.TemporaryDirectory(prefix="gtfs_rt_aggregator-tse-") as tmp:
            files = []
            for offset in (-1, 0, 1):
                for path in self._day_files(
                    storage, provider.name, day + timedelta(days=offset), tz
                ):
                    local = os.path.join(tmp, f"{len(files)}.parquet")
                    storage.read_to_file(path, local)
                    files.append(local)

            timetables = {}
            for version in self._versions(storage, provider.name, static_name, files):
                folder = self._download_static(
                    storage, provider.name, static_name, version, tmp
                )
                if folder is not None:
                    timetables[version] = StaticTimetable(folder)
            frame = build_trip_stop_events(
                files, day, provider.timezone, provider.name, timetables
            )
            if frame is None:
                return False
            output = os.path.join(tmp, "events.parquet")
            pq.write_table(
                _plain_types(frame.to_arrow()),
                output,
                compression="zstd",
                row_group_size=100_000,
            )
            target = self.output_path(provider.name, day, tz)
            storage.save_file(output, target)
            logger.info(f"{SERVICE}: wrote {target} ({frame.height} rows)")
        return True

    @staticmethod
    def _versions(storage, provider_name, static_name, files) -> List[str]:
        """Static versions named by the updates, else the latest one; oldest first."""
        versions = set()
        for path in files:
            if "staticVersion" in pq.read_schema(path).names:
                column = pq.read_table(path, columns=["staticVersion"])["staticVersion"]
                versions.update(v for v in column.unique().to_pylist() if v)
        if not versions:
            latest = read_latest(
                storage, static_base(provider_name, static_name), logger
            )
            if latest and latest.get("version"):
                versions.add(latest["version"])
        # Version names are UTC times: sorting them sorts by date
        return sorted(versions)

    @staticmethod
    def _download_static(storage, provider_name, static_name, version, tmp):
        base = static_base(provider_name, static_name)
        manifest_path = f"{base}/{version}/manifest.json"
        if not storage.file_exists(manifest_path):
            logger.warning(f"{SERVICE}: static version {manifest_path} not found")
            return None
        tables = manifest_tables(json.loads(storage.read_bytes(manifest_path)), base)
        if "stop_times" not in tables:
            logger.warning(f"{SERVICE}: static version {version} has no stop_times")
            return None
        folder = os.path.join(tmp, "static", version)
        os.makedirs(folder, exist_ok=True)
        for name in STATIC_TABLES:
            if name in tables:
                storage.read_to_file(
                    tables[name], os.path.join(folder, f"{name}.parquet")
                )
        return folder
