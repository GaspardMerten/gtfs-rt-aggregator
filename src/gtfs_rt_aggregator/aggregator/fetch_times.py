"""
Fetch times of a file, by feedId, kept in its Parquet metadata.

Deduplication ends a run when an entity is missing from a fetch. Once rows are
merged, the fetch times where an entity was missing are no longer in the rows
(an entity seen at 10:00 and 10:02 but not at 10:01 keeps two rows; merging
them again would find no 10:01 fetch between them). So every file written
since 0.7.4 lists all its fetch times, including fetches with no entity, in its
metadata; files merged together carry the union of their lists.
"""

import json
from typing import Dict, Iterable, Optional, Set

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

FETCH_TIMES_METADATA = b"gtfs_rt_aggregator.fetch_times"
TIMESTAMP = pa.timestamp("us", tz="UTC")

# Epoch microseconds by feedId (None: rows written before 0.7.3, no feedId)
TimeSets = Dict[Optional[str], Set[int]]


def encode(times: TimeSets) -> Dict[bytes, bytes]:
    """Metadata entry holding times (each list delta-encoded, to stay small)."""
    value = {}
    for feed, values in times.items():
        ordered = sorted(values)
        value["" if feed is None else feed] = ordered[:1] + [
            b - a for a, b in zip(ordered, ordered[1:])
        ]
    return {FETCH_TIMES_METADATA: json.dumps(value, separators=(",", ":")).encode()}


def decode(metadata: Optional[Dict[bytes, bytes]]) -> Optional[TimeSets]:
    """Times recorded in a schema's metadata (None if the file has none)."""
    raw = (metadata or {}).get(FETCH_TIMES_METADATA)
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
    columns = [
        c for c in ("fetchTime", "firstSeen", "lastSeen", "feedId") if c in schema.names
    ]
    for batch in pq.ParquetFile(path).iter_batches(columns=columns, batch_size=262_144):
        from_table(pa.Table.from_batches([batch]), times)
    return times


def of_fetch(feed: Optional[str], fetch_time) -> TimeSets:
    """Times of a single fetch (fetch_time: an aware datetime)."""
    return {feed: _epoch_us(pa.array([fetch_time], TIMESTAMP))}


def to_arrays(times: TimeSets) -> Dict[Optional[str], pa.Array]:
    """Sorted timestamp arrays by feedId, as deduplicate() takes them."""
    return {
        feed: pc.cast(pa.array(sorted(values), pa.int64()), TIMESTAMP)
        for feed, values in times.items()
    }


def with_times(table: pa.Table, times: Optional[TimeSets]) -> pa.Table:
    """table with times added to its schema metadata."""
    if not times:
        return table
    metadata = dict(table.schema.metadata or {})
    metadata.update(encode(times))
    return table.replace_schema_metadata(metadata)
