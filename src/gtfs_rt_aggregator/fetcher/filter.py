import hashlib
import json
import os
import tempfile
from io import BytesIO
from typing import Dict, Optional, Set, Tuple

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from google.transit import gtfs_realtime_pb2

from ..config.models import FilterConfig
from ..storage.base import StorageInterface

# Changed when the resolution changes (2: route_type no longer truncated by
# gtfs-parquet < 0.6.1; 6: trips and routes of the other modes), so earlier
# cached results are not reused
CACHE_FORMAT = 6

# Key of the tables given to build_filter (see static.service.filter_tables):
# the side table of the trips and routes a filtered static version dropped
# (the other modes)
OTHER_MODES = "_other_modes"

# Trips that are not in the static timetable by nature
_ADDED = {
    gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Value(name)
    for name in ("ADDED", "NEW", "DUPLICATED")
    if name in gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.keys()
}


def trip_key(trip_id: str) -> int:
    """64-bit key of a trip id: the trips of the other modes are kept as a
    sorted array of these (millions on national feeds, 8 bytes each instead
    of a Python string in a set)."""
    return int.from_bytes(
        hashlib.blake2b(trip_id.encode(), digest_size=8).digest(), "little"
    )


def trip_keys(trip_ids: pa.ChunkedArray, batch: int = 100_000) -> np.ndarray:
    """Sorted unique keys of trip ids (nulls skipped), a batch at a time so
    that millions of ids are never all Python strings at once."""
    parts = []
    for offset in range(0, len(trip_ids), batch):
        ids = trip_ids.slice(offset, batch).drop_null().cast(pa.string()).to_pylist()
        parts.append(np.fromiter((trip_key(t) for t in ids), np.uint64, len(ids)))
    return np.unique(np.concatenate(parts)) if parts else np.empty(0, np.uint64)


class EntityFilter:
    """
    Decides which GTFS-RT entities to keep, before their rows are built.

    Route ids and route types are resolved through a static version: routes
    gives the type of each route, trips the route of each trip (realtime
    entities often carry only a trip id).

    With other_routes and other_trips (every route and trip of the full static
    feed that is not kept, i.e. of another mode), only entities known to be of
    another mode are dropped: a trip and route the static feed does not know
    (a timetable not stored yet, an extra train) is kept. It may also be a bus
    added by the operator: kept too, few in practice. Alerts follow the same
    rule. They are given only to a filter with route_types on a version
    stored with static route_types (its side table lists every trip of the
    other modes of the same mixed feed as the realtime).

    In every other case an unknown trip cannot be told from a trip of another
    mode and is dropped, except added trips with keep_unmatched_added: a
    version stored whole (it may be one operator's timetable, with a few
    replacement buses, next to a realtime feed of every operator), a filter
    by route_ids alone (it asks for those routes only), and a version
    filtered before 0.13.0 that has no side table yet.
    """

    def __init__(
        self,
        route_types: Set[int],
        routes: Set[str],
        trips: Set[str],
        known_routes: Optional[Set[str]] = None,
        keep_unmatched_added: bool = False,
        known_trips: Optional[Set[str]] = None,
        other_routes: Optional[Set[str]] = None,
        other_trips: Optional[np.ndarray] = None,
        side_trips: Optional[np.ndarray] = None,
    ):
        self.route_types = route_types
        self.routes = routes - {""}
        self.trips = trips - {""}
        # Every route of the static feed, to tell unknown routes from unwanted ones
        self.known_routes = (known_routes or set()) - {""}
        # Every trip of the static feed: a DUPLICATED trip points at one
        self.known_trips = (known_trips or set()) - {""}
        self.keep_unmatched_added = keep_unmatched_added
        # Routes and trips (sorted trip_key values) of the other modes; None when
        # the static version does not tell them (only unknown or kept trips known)
        self.knows_other_modes = other_routes is not None
        self.other_routes = (other_routes or set()) - {""}
        self.other_trips = _keys(other_trips)
        # Trips of the side table kept (route_ids, or route_types wider than the
        # static ones): trip keys, as there may be millions
        self.side_trips = _keys(side_trips)

    def keep(self, entity) -> bool:
        if entity.HasField("alert"):
            return any(self._keep_selector(s) for s in entity.alert.informed_entity)
        return self.classify(entity)[0]

    def _keep_selector(self, selector) -> bool:
        """An alert's informed entity: matched as trips are (see _classify_trip)."""
        key = self._key(selector.trip.trip_id)
        if (
            selector.route_id in self.routes
            or selector.trip.trip_id in self.trips
            or selector.trip.route_id in self.routes
            or _has(self.side_trips, key)
            or (selector.HasField("route_type") and selector.route_type in self.route_types)
        ):
            return True
        if not self.knows_other_modes:
            return False
        # An informed trip or route not known as another mode is kept; one with
        # nothing to match (a stop, an agency) or of another route type is not
        if not (selector.route_id or selector.trip.route_id or selector.trip.trip_id):
            return False
        if selector.HasField("route_type"):
            return False
        return not (
            selector.route_id in self.other_routes
            or selector.trip.route_id in self.other_routes
            or _has(self.other_trips, key)
        )

    def unknown(self, entity) -> bool:
        """
        Whether an entity's trip is unknown to the static version: neither a
        kept trip or route nor (when the version tells them) one of another
        mode. Many unknown trips mean the static version is behind the
        realtime feed. They are kept when the version tells the other modes,
        dropped otherwise: a static version stored for route_types only (rail)
        before 0.13.0 knows no trip of the other modes, which are unknown too.
        """
        return self.classify(entity)[1]

    def classify(self, entity) -> Tuple[bool, bool]:
        """keep() and unknown() at once (the trip id hashed once)."""
        if entity.HasField("trip_update"):
            return self._classify_trip(entity.trip_update.trip)
        if entity.HasField("vehicle"):
            # A vehicle not assigned to a trip cannot be matched
            if not entity.vehicle.HasField("trip"):
                return False, False
            return self._classify_trip(entity.vehicle.trip)
        if entity.HasField("alert"):
            return self.keep(entity), False
        # Other entity types (trip modifications, shapes, stops) are kept
        return True, False

    def _key(self, trip_id: str):
        """trip_key of a trip id, only when there are keys to look it up in."""
        if trip_id and (len(self.other_trips) or len(self.side_trips)):
            return np.uint64(trip_key(trip_id))
        return None

    def _classify_trip(self, trip) -> Tuple[bool, bool]:
        if not (trip.trip_id or trip.route_id):
            return False, False  # nothing to match
        key = self._key(trip.trip_id)
        if (
            trip.trip_id in self.trips
            or trip.route_id in self.routes
            or _has(self.side_trips, key)
        ):
            return True, False
        if self.knows_other_modes:
            # Dropped only when known to be of another mode (a DUPLICATED
            # trip pointing at a bus trip too); unknown: kept
            other = (trip.route_id and trip.route_id in self.other_routes) or _has(
                self.other_trips, key
            )
            return not other, not other
        unknown = (
            trip.route_id not in self.known_routes
            and (not self.known_trips or trip.trip_id not in self.known_trips)
        )
        # An added trip is not in the static timetable: keep it when its route
        # cannot be resolved either (missing, or unknown to the static feed)
        keep = (
            self.keep_unmatched_added
            and trip.schedule_relationship in _ADDED
            and trip.route_id not in self.known_routes
            and trip.trip_id not in self.known_trips
        )
        return keep, unknown


def _keys(keys: Optional[np.ndarray]) -> np.ndarray:
    return np.empty(0, np.uint64) if keys is None else keys


def _has(keys: np.ndarray, key) -> bool:
    """Whether a trip key (None: no trip id) is among sorted trip keys."""
    if key is None or not len(keys):
        return False
    i = int(np.searchsorted(keys, key))
    return i < len(keys) and keys[i] == key


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
        no version is stored yet. OTHER_MODES, if present, is the path of the
        side table of the other modes a version stored with static route_types
        holds: filters with route_types then keep unknown trips. Absent (a
        version stored whole, or filtered before 0.13.0): unknown trips cannot
        be told from other modes and are dropped (see EntityFilter).
    @param cache_key: Identifies the static version, for the local cache
    @return The filter, or None if it needs a static version and none is stored
    """
    route_types = config.route_type_set()
    if not config.needs_static:
        return EntityFilter(route_types, set(), set(config.trip_ids))
    if not tables or "routes" not in tables or "trips" not in tables:
        return None

    # Resolved once per static version and filter, kept in this process, and on
    # local disk for the other workers. CACHE_FORMAT changes when resolution
    # changes, so old results are not reused. The side table is in the key: it
    # can be added to a version already stored (see StaticService.process).
    digest = hashlib.sha1(
        f"{CACHE_FORMAT}|{cache_key}|{tables.get(OTHER_MODES)}|"
        f"{config.model_dump_json()}".encode()
    ).hexdigest()
    if digest in _MEMO:
        return _MEMO[digest]
    # Per user: files other users can write must not decide what is filtered
    cache = os.path.join(
        tempfile.gettempdir(), f"gtfs_rt_aggregator-{_user()}", f"filter-{digest}.json"
    )
    keys_cache = f"{cache[: -len('.json')]}.npz"
    names = ("routes", "trips", "known_routes", "known_trips", "other_routes")
    try:
        with open(cache) as f:
            cached = json.load(f)
        resolved = tuple(
            None if cached[name] is None else set(cached[name]) for name in names
        )
        if cached["other_routes"] is not None:
            with np.load(keys_cache) as keys:
                resolved += (keys["other_trips"], keys["side_trips"])
        else:
            resolved += (None, None)
    except Exception:
        # Missing, or cut by a crash (a bad zip, EOF...): resolved and written again
        resolved = _resolve(config, route_types, storage, tables)
        try:
            os.makedirs(os.path.dirname(cache), mode=0o700, exist_ok=True)
            if resolved[-2] is not None:
                # Before the json: a json without its keys would be read as complete
                # A unique name per writer (processes, and threads of one)
                fd, tmp = tempfile.mkstemp(dir=os.path.dirname(cache), suffix=".npz.tmp")
                with os.fdopen(fd, "wb") as f:
                    np.savez(f, other_trips=resolved[-2], side_trips=resolved[-1])
                os.replace(tmp, keys_cache)
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(cache), suffix=".json.tmp")
            with os.fdopen(fd, "w") as f:
                json.dump(
                    {
                        name: None if values is None else sorted(values)
                        for name, values in zip(names, resolved[: len(names)])
                    },
                    f,
                )
            os.replace(tmp, cache)
        except OSError:
            # Only a cache (e.g. the folder belongs to another user)
            pass
    (
        routes,
        trips,
        known_routes,
        known_trips,
        other_routes,
        other_trips,
        side_trips,
    ) = resolved
    entity_filter = EntityFilter(
        route_types,
        routes,
        trips,
        known_routes,
        config.keep_unmatched_added,
        known_trips,
        other_routes,
        other_trips,
        side_trips,
    )
    if len(_MEMO) >= 16:
        _MEMO.pop(next(iter(_MEMO)))
    _MEMO[digest] = entity_filter
    return entity_filter


# Filters of this process, by digest (a worker processes many fetches)
_MEMO: Dict[str, EntityFilter] = {}


def _user() -> str:
    try:
        return str(os.getuid())
    except AttributeError:  # Windows
        import getpass

        return getpass.getuser()


def _read(storage: StorageInterface, path: str, columns) -> pa.Table:
    return pq.read_table(BytesIO(storage.read_bytes(path)), columns=columns)


def _resolve(
    config: FilterConfig,
    route_types: Set[int],
    storage: StorageInterface,
    tables: Dict[str, str],
) -> Tuple[
    Set[str],
    Set[str],
    Set[str],
    Set[str],
    Optional[Set[str]],
    Optional[np.ndarray],
    Optional[np.ndarray],
]:
    """Route ids and trip ids to keep, all route ids, all trip ids (only for
    keep_unmatched_added), and the route ids and trip keys of the other modes
    and the trip keys of the side table kept (None when the static version
    does not tell the other modes, or the filter has no route_types), from
    the static routes and trips tables and the side table of the other modes."""
    routes_table = _read(storage, tables["routes"], ["route_id", "route_type"])
    routes = set(config.route_ids)
    if route_types:
        mask = pc.is_in(
            routes_table["route_type"].cast(pa.int64()),
            value_set=pa.array(sorted(route_types), pa.int64()),
        )
        routes.update(routes_table.filter(mask)["route_id"].cast(pa.string()).to_pylist())
    # A null route id (a broken feed) matches nothing
    routes.discard(None)

    trips_table = _read(storage, tables["trips"], ["trip_id", "route_id"])
    # Polars may write large_string columns: compare as plain strings
    trip_routes = trips_table["route_id"].cast(pa.string())
    mask = pc.is_in(trip_routes, value_set=pa.array(sorted(routes), pa.string()))
    trips = set(config.trip_ids)
    trips.update(trips_table.filter(mask)["trip_id"].cast(pa.string()).to_pylist())
    trips.discard(None)
    known_routes = set(routes_table["route_id"].cast(pa.string()).to_pylist())
    known_routes.discard(None)

    # Unknown trips are kept only by a mode filter (a route_ids filter asks for its
    # routes only), on a version with the side table: stored with route_types, it
    # is the same mixed feed as the realtime and lists every trip of the other
    # modes. A version stored whole may be one operator's next to a realtime feed
    # of every operator (a rail timetable with a few replacement buses): a bus
    # missing from it cannot be told from an unknown train
    other_modes = tables.get(OTHER_MODES) if route_types else None
    if not other_modes:
        # Only needed (and large: millions on national feeds) for keep_unmatched_added
        known_trips = (
            set(trips_table["trip_id"].cast(pa.string()).to_pylist())
            if config.keep_unmatched_added
            else set()
        )
        return routes, trips, known_routes, known_trips, None, None, None

    # Not kept: of another mode. The version's own routes and trips not kept (a
    # filter narrower than the static route_types), and those the static
    # route_types dropped (side table). A trip whose route is missing from
    # routes is not known as another mode.
    other_routes = known_routes - routes
    other = pc.and_(
        pc.invert(pc.fill_null(mask, False)),
        pc.fill_null(
            pc.is_in(trip_routes, value_set=pa.array(sorted(known_routes), pa.string())),
            False,
        ),
    )
    other_ids = [trips_table.filter(other)["trip_id"]]
    side_ids = []
    side = _read(storage, other_modes, ["trip_id", "route_id", "route_type"])
    side_routes = side["route_id"].cast(pa.string())
    # Rows the filter asks for (its route_ids, or its route_types wider than
    # the static ones) are kept, not another mode
    wanted = pc.fill_null(
        pc.or_(
            pc.is_in(side_routes, value_set=pa.array(sorted(routes), pa.string())),
            pc.is_in(
                side["route_type"].cast(pa.int64()),
                value_set=pa.array(sorted(route_types), pa.int64()),
            ),
        ),
        False,
    )
    kept_side = side.filter(wanted)
    routes |= set(kept_side["route_id"].drop_null().cast(pa.string()).to_pylist())
    side_ids.append(kept_side["trip_id"])
    others = side.filter(pc.invert(wanted))
    other_routes |= set(others["route_id"].drop_null().cast(pa.string()).to_pylist())
    other_ids.append(others["trip_id"])
    # (route_ids and trip_ids of the config are matched first: kept anyway)
    return (
        routes,
        trips,
        known_routes,
        set(),
        other_routes,
        trip_keys(_strings(other_ids)),
        trip_keys(_strings(side_ids)),
    )


def _strings(columns) -> pa.ChunkedArray:
    return pa.chunked_array(
        [chunk for ids in columns for chunk in ids.cast(pa.string()).chunks],
        pa.string(),
    )
