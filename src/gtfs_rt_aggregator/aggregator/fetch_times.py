"""
Fetch times of a file, by feedId, kept in its Parquet metadata.

Deduplication ends a run when an entity is missing from a fetch. Once rows are
merged, the fetch times where an entity was missing are no longer in the rows
(an entity seen at 10:00 and 10:02 but not at 10:01 keeps two rows; merging
them again would find no 10:01 fetch between them). So every file written
since 0.7.4 lists all its fetch times, including fetches with no entity, in its
metadata; files merged together carry the union of their lists.

Fetches whose entities were the same as the previous fetch's (skip_unchanged)
write no rows. Their times are kept apart, in a second metadata entry
(UNCHANGED_TIMES_METADATA, since 0.9.4): every entity of the previous fetch
was still there, so they are not gaps, but they tell a feed that was read
and did not change from one that was not read at all (see dedup.py).
"""

import json
from typing import Dict, Iterable, Optional, Set

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

FETCH_TIMES_METADATA = b"gtfs_rt_aggregator.fetch_times"
UNCHANGED_TIMES_METADATA = b"gtfs_rt_aggregator.unchanged_fetch_times"
# Random id of each written file: tells apart two files with the same name
# and size (see service.SOURCES_METADATA)
WRITE_ID_METADATA = b"gtfs_rt_aggregator.write_id"
TIMESTAMP = pa.timestamp("us", tz="UTC")

# Epoch microseconds by feedId (None: rows written before 0.7.3, no feedId)
TimeSets = Dict[Optional[str], Set[int]]


def encode(times: TimeSets, key: bytes = FETCH_TIMES_METADATA) -> Dict[bytes, bytes]:
    """Metadata entry holding times (each list delta-encoded, to stay small)."""
    value = {}
    for feed, values in times.items():
        ordered = sorted(values)
        value["" if feed is None else feed] = ordered[:1] + [
            b - a for a, b in zip(ordered, ordered[1:])
        ]
    return {key: json.dumps(value, separators=(",", ":")).encode()}


def decode(
    metadata: Optional[Dict[bytes, bytes]], key: bytes = FETCH_TIMES_METADATA
) -> Optional[TimeSets]:
    """Times recorded in a schema's metadata (None if the file has none)."""
    raw = (metadata or {}).get(key)
    if raw is None:
        return None
    times = {}
    for feed, deltas in json.loads(raw).items():
        values, total = set(), 0
        for delta in deltas:
            total += delta
            values.add(total)
        times[feed or None] = values
    return times


def merge(into: TimeSets, other: Optional[TimeSets]) -> TimeSets:
    for feed, values in (other or {}).items():
        into.setdefault(feed, set()).update(values)
    return into


def split_by_feed(table: pa.Table) -> Iterable:
    """(feedId, rows of that feed) for each feed of a table."""
    if "feedId" not in table.column_names:
        yield None, table
        return
    feeds = pc.unique(table["feedId"]).to_pylist()
    if len(feeds) <= 1:
        yield (feeds[0] if feeds else None), table
        return
    for feed in feeds:
        mask = (
            pc.is_null(table["feedId"])
            if feed is None
            else pc.fill_null(pc.equal(table["feedId"], feed), False)
        )
        yield feed, table.filter(mask)


def _epoch_us(column) -> Set[int]:
    values = pc.unique(pc.cast(pc.cast(column, TIMESTAMP), pa.int64()))
    return {v for v in values.to_pylist() if v is not None}


def from_table(table: pa.Table, times: Optional[TimeSets] = None) -> TimeSets:
    """Add the times found in a table's rows (fetchTime, firstSeen, lastSeen)."""
    times = {} if times is None else times
    names = [
        c for c in ("fetchTime", "firstSeen", "lastSeen") if c in table.column_names
    ]
    for feed, rows in split_by_feed(table):
        found = times.setdefault(feed, set())
        for name in names:
            found.update(_epoch_us(rows[name]))
    return times


def of_file(path: str, times: Optional[TimeSets] = None) -> TimeSets:
    """Add a local Parquet file's times: its metadata, and its rows."""
    times = {} if times is None else times
    schema = pq.read_schema(path)
    merge(times, decode(schema.metadata))
    # Times stored as Unix seconds (before 0.6.0) are read after conform
    columns = [
        c
        for c in ("fetchTime", "firstSeen", "lastSeen")
        if c in schema.names and pa.types.is_timestamp(schema.field(c).type)
    ]
    if not columns:
        return times
    if "feedId" in schema.names:
        columns.append("feedId")
    for batch in pq.ParquetFile(path).iter_batches(columns=columns, batch_size=262_144):
        from_table(pa.Table.from_batches([batch]), times)
    return times


def unchanged_of_file(path: str, times: Optional[TimeSets] = None) -> TimeSets:
    """Add a local Parquet file's unchanged fetch times (none before 0.9.4)."""
    times = {} if times is None else times
    return merge(times, decode(pq.read_schema(path).metadata, UNCHANGED_TIMES_METADATA))


def of_fetch(feed: Optional[str], fetch_time) -> TimeSets:
    """Times of a single fetch (fetch_time: an aware datetime)."""
    return {feed: _epoch_us(pa.array([fetch_time], TIMESTAMP))}


def to_arrays(times: TimeSets) -> Dict[Optional[str], pa.Array]:
    """Sorted timestamp arrays by feedId, as deduplicate() takes them."""
    return {
        feed: pc.cast(pa.array(sorted(values), pa.int64()), TIMESTAMP)
        for feed, values in times.items()
    }


def write_id() -> Dict[bytes, bytes]:
    import uuid

    return {WRITE_ID_METADATA: uuid.uuid4().hex.encode()}


def with_times(
    table: pa.Table, times: Optional[TimeSets], unchanged: Optional[TimeSets] = None
) -> pa.Table:
    """table with times, unchanged fetch times (and a new write id) added to its schema metadata."""
    metadata = dict(table.schema.metadata or {})
    metadata.pop(UNCHANGED_TIMES_METADATA, None)
    metadata.update(encode(times or {}))
    if unchanged:
        metadata.update(encode(unchanged, UNCHANGED_TIMES_METADATA))
    metadata.update(write_id())
    return table.replace_schema_metadata(metadata)


# Unchanged fetches wait in the feed's state for the next file written; a feed
# that stays unchanged writes a file with no rows holding them this often, so
# they reach the files of their own period
UNCHANGED_FLUSH_SECONDS = 600


def note_unchanged(state: dict, fetch_times_iso: Iterable[str]) -> None:
    """Remember unchanged fetches (ISO times) in a feed's state, until written."""
    pending = state.setdefault("unchanged", [])
    for value in fetch_times_iso:
        pending.extend(_epoch_us(pa.array([value], pa.string()).cast(TIMESTAMP)))


def pending_unchanged(state: dict, feed: Optional[str]) -> Optional[TimeSets]:
    """The unchanged fetches waiting in a feed's state, as TimeSets (None if none)."""
    values = state.get("unchanged")
    return {feed: set(values)} if values else None


def unchanged_flush_due(state: dict, fetch_time) -> bool:
    """Whether the oldest unchanged fetch waiting is UNCHANGED_FLUSH_SECONDS old."""
    values = state.get("unchanged")
    return bool(values) and (
        fetch_time.timestamp() * 1_000_000 - min(values)
        >= UNCHANGED_FLUSH_SECONDS * 1_000_000
    )


def worth_storing(tables: Dict[str, pa.Table], state: dict) -> Dict[str, pa.Table]:
    """
    The tables of a fetch to store. A service with no rows is stored (as a
    file with no rows, holding the fetch time) only when the previous stored
    fetch had rows of it: that one fetch time is enough to end the runs of
    its entities; the following empty fetches would only add files. state
    (the feed's, saved by the caller) remembers the empty services.
    """
    empty_before = set(state.get("empty_services", []))
    state["empty_services"] = sorted(
        service for service, table in tables.items() if table.num_rows == 0
    )
    return {
        service: table
        for service, table in tables.items()
        if table.num_rows or service not in empty_before
    }
