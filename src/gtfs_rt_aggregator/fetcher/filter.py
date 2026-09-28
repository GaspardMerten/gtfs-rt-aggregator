import hashlib
import json
import os
import tempfile
from io import BytesIO
from typing import Dict, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..config.models import FilterConfig
from ..storage.base import StorageInterface

# Changed when the resolution changes (2: route_type no longer truncated by
# gtfs-parquet < 0.6.1), so earlier cached results are not reused
CACHE_FORMAT = 2


class EntityFilter:
    """
    Decides which GTFS-RT entities to keep, before they are converted to dicts.

    Route ids and route types are resolved through a static version: routes
    gives the type of each route, trips the route of each trip (realtime
    entities often carry only a trip id).
    """

    def __init__(self, route_types: Set[int], routes: Set[str], trips: Set[str]):
        self.route_types = route_types
        self.routes = routes - {""}
        self.trips = trips - {""}

    def keep(self, entity) -> bool:
        if entity.HasField("trip_update"):
            return self._keep_trip(entity.trip_update.trip)
        if entity.HasField("vehicle"):
            # A vehicle not assigned to a trip cannot be matched
            return entity.vehicle.HasField("trip") and self._keep_trip(
                entity.vehicle.trip
            )
        if entity.HasField("alert"):
            return any(
                selector.route_id in self.routes
                or selector.trip.trip_id in self.trips
                or selector.trip.route_id in self.routes
                or (
                    selector.HasField("route_type")
                    and selector.route_type in self.route_types
                )
                for selector in entity.alert.informed_entity
            )
        # Other entity types (trip modifications, shapes, stops) are kept
        return True

    def _keep_trip(self, trip) -> bool:
        return trip.trip_id in self.trips or trip.route_id in self.routes


def build_filter(
    config: FilterConfig,
    storage: Optional[StorageInterface],
    tables: Optional[Dict[str, str]],
    cache_key: str,
) -> Optional[EntityFilter]:
    """
    Build the filter of a realtime feed.

    @param config: Filter configuration
    @param storage: Storage holding the static version (if the filter needs one)
    @param tables: Storage path of each table of the static version, or None if
        no version is stored yet
    @param cache_key: Identifies the static version, for the local cache
    @return The filter, or None if it needs a static version and none is stored
    """
    route_types = config.route_type_set()
    if not config.needs_static:
        return EntityFilter(route_types, set(), set(config.trip_ids))
    if not tables or "routes" not in tables or "trips" not in tables:
        return None

    # Resolved once per static version and filter, then read from local disk:
    # each fetch runs in its own process
    # CACHE_FORMAT changes when resolution changes, so old results are not reused
    digest = hashlib.sha1(
        f"{CACHE_FORMAT}|{cache_key}|{config.model_dump_json()}".encode()
    ).hexdigest()
    # Per user: files other users can write must not decide what is filtered
    cache = os.path.join(
        tempfile.gettempdir(), f"gtfs_rt_aggregator-{_user()}", f"filter-{digest}.json"
    )
    try:
        with open(cache) as f:
            cached = json.load(f)
        return EntityFilter(route_types, set(cached["routes"]), set(cached["trips"]))
    except (OSError, ValueError, KeyError):
        pass

    routes, trips = _resolve(config, route_types, storage, tables)
    try:
        os.makedirs(os.path.dirname(cache), mode=0o700, exist_ok=True)
        tmp = f"{cache}.tmp-{os.getpid()}"
        with open(tmp, "w") as f:
            json.dump({"routes": sorted(routes), "trips": sorted(trips)}, f)
        os.replace(tmp, cache)
    except OSError:
        # Only a cache (e.g. the folder belongs to another user)
        pass
    return EntityFilter(route_types, routes, trips)


def _user() -> str:
    try:
        return str(os.getuid())
    except AttributeError:  # Windows
        import getpass

        return getpass.getuser()


def _resolve(
    config: FilterConfig,
    route_types: Set[int],
    storage: StorageInterface,
    tables: Dict[str, str],
) -> Tuple[Set[str], Set[str]]:
    """Route ids and trip ids to keep, from the static routes and trips tables."""
    routes_table = pq.read_table(
        BytesIO(storage.read_bytes(tables["routes"])),
        columns=["route_id", "route_type"],
    )
    routes = set(config.route_ids)
    if route_types:
        mask = pc.is_in(
            routes_table["route_type"].cast(pa.int64()),
            value_set=pa.array(sorted(route_types), pa.int64()),
        )
        routes.update(routes_table.filter(mask)["route_id"].to_pylist())

    trips_table = pq.read_table(
        BytesIO(storage.read_bytes(tables["trips"])), columns=["trip_id", "route_id"]
    )
    # Polars may write large_string columns: compare as plain strings
    mask = pc.is_in(
        trips_table["route_id"].cast(pa.string()),
        value_set=pa.array(sorted(routes), pa.string()),
    )
    trips = set(config.trip_ids)
    trips.update(trips_table.filter(mask)["trip_id"].to_pylist())
    return routes, trips
