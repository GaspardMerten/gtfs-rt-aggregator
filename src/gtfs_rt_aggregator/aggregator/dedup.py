from typing import Dict, Optional, Tuple, Union

import pyarrow as pa
import pyarrow.compute as pc

from .fetch_times import split_by_feed

# Fetch times of each feed (feedId, None for rows without one)
FeedTimes = Dict[Optional[str], pa.Array]
# Longest wait in seconds between two fetches of a feed (by feedId) still
# counted as reading it without interruption
MaxGap = Union[float, Dict[Optional[str], float], None]

# A run ends where its feed was not read for longer than the larger of
# MIN_GAP_SECONDS and GAP_REFRESHES times its refresh_seconds
MIN_GAP_SECONDS = 600
GAP_REFRESHES = 3

# Besides entityId, rows of a TripUpdate are told apart by their trip: some
# feeds repeat an entity id for runs of one trip on different days
TRIP_KEYS = ("trip_tripId", "trip_startDate")

_UNITS_PER_SECOND = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}


def max_gap_seconds(refresh_seconds: float) -> float:
    """Longest wait between two fetches that does not end a run (see _deduplicate_feed)."""
    return max(GAP_REFRESHES * refresh_seconds, MIN_GAP_SECONDS)


def deduplicate(
    table: pa.Table,
    times: Union[pa.Array, FeedTimes, None] = None,
    unchanged: Union[pa.Array, FeedTimes, None] = None,
    max_gap: MaxGap = None,
) -> pa.Table:
    """
    Merge consecutive rows of the same entity with the same content, feed by
    feed: a provider can have several feeds of the same service (e.g. one per
    operator), with their own fetch times and entity ids. See
    _deduplicate_feed. Returns rows sorted by entityId and firstSeen.
    """
    parts = [
        _deduplicate_feed(
            rows,
            _feed_times(times, feed),
            _feed_times(unchanged, feed),
            _feed_gap(max_gap, feed),
        )
        for feed, rows in split_by_feed(table)
    ]
    if len(parts) == 1:
        return parts[0]
    result = pa.concat_tables(parts)
    return result.take(
        pc.sort_indices(
            result, sort_keys=[("entityId", "ascending"), ("firstSeen", "ascending")]
        )
    )


def _feed_times(times, feed) -> Optional[pa.Array]:
    if isinstance(times, dict):
        return times.get(feed)
    return times


def _feed_gap(max_gap: MaxGap, feed) -> float:
    if isinstance(max_gap, dict):
        # A feed no longer configured: the shortest wait
        max_gap = max_gap.get(feed)
    return MIN_GAP_SECONDS if max_gap is None else max_gap


def _as_units(values: pa.Array) -> Tuple[pa.Array, int]:
    """Times as int64, and how many make a second (int times: Unix seconds)."""
    if pa.types.is_timestamp(values.type):
        return values.cast(pa.int64()), _UNITS_PER_SECOND[values.type.unit]
    return values.cast(pa.int64()), 1


def _deduplicate_feed(
    table: pa.Table,
    times: Optional[pa.Array] = None,
    unchanged: Optional[pa.Array] = None,
    max_gap: float = MIN_GAP_SECONDS,
) -> pa.Table:
    """
    Merge consecutive rows of the same entity with the same content.

    Rows are compared on entityId and contentHash (a hash of the whole entity,
    set when fetching). Each run of identical consecutive rows of an entity
    becomes its first row, with firstSeen and lastSeen set to the first and
    last fetchTime of the run. A run ends when the entity changes, or is missing
    from a fetch time found in the table (it disappeared, then came back).
    Tables that are already deduplicated (they have firstSeen and lastSeen) can
    be merged again with new rows.

    Rows without contentHash (written before 0.5.0) are kept as they are.

    A run also ends where the feed was not read for longer than max_gap
    seconds (an outage of the feed or of the pipeline): consecutive fetch
    times, unchanged ones included, further apart than that. Without that,
    an entity seen before and after a 7-hour outage would look present all
    along (a frozen feed).

    TripUpdate rows (with trip_tripId and trip_startDate) are compared on
    entityId, trip and start date: a feed may reuse an entity id for the same
    trip on two days in one fetch.

    times: fetch times to consider for gaps besides those found in the rows:
    the fetch times recorded in the files' metadata (see fetch_times.py), or
    those of the whole day when the table is only part of it (streaming
    compaction). unchanged: fetches whose entities were the same as the
    previous fetch's (no rows written): only used for max_gap.
    """
    # New rows (no firstSeen / lastSeen yet, or null after being concatenated
    # with a deduplicated file) were seen once, at fetchTime
    for column in ("firstSeen", "lastSeen"):
        if column in table.column_names:
            values = pc.coalesce(table[column], table["fetchTime"])
            table = table.set_column(
                table.schema.get_field_index(column), column, values
            )
        else:
            table = table.append_column(column, table["fetchTime"])
    if table.num_rows < 2 or "contentHash" not in table.column_names:
        return table

    trip_keys = [k for k in TRIP_KEYS if k in table.column_names]
    if len(trip_keys) < len(TRIP_KEYS):
        trip_keys = []
    table = table.take(
        pc.sort_indices(
            table,
            sort_keys=[("entityId", "ascending")]
            + [(k, "ascending") for k in trip_keys]
            + [("firstSeen", "ascending")],
        )
    ).combine_chunks()
    entity = table["entityId"].chunk(0).cast(pa.string())
    # All-null hashes have the null type, which cannot be compared
    content = table["contentHash"].chunk(0).cast(pa.string())
    same_entity = pc.equal(entity[1:], entity[:-1])
    for key in trip_keys:
        # A missing trip id or date matches another missing one
        values = pc.fill_null(table[key].chunk(0).cast(pa.string()), "")
        same_entity = pc.and_(same_entity, pc.equal(values[1:], values[:-1]))

    # Every fetch time, in order: an entity missing from a fetch between two
    # of its rows ends its run there
    found = [
        table["fetchTime"].combine_chunks(),
        table["firstSeen"].combine_chunks(),
        table["lastSeen"].combine_chunks(),
    ]
    if times is not None:
        found.append(times.cast(table["firstSeen"].type))
    times = _sorted_unique(found)
    first_rank = pc.index_in(table["firstSeen"].chunk(0), value_set=times)
    last_rank = pc.index_in(table["lastSeen"].chunk(0), value_set=times)
    adjacent = pc.less_equal(
        pc.subtract(first_rank[1:], last_rank[:-1]), pa.scalar(1, first_rank.type)
    )

    # Every time the feed was read, unchanged fetches included, split where
    # it was not read for longer than max_gap: two rows of a run must be in
    # the same stretch
    read = [times]
    if unchanged is not None:
        read.append(unchanged.cast(table["firstSeen"].type))
    read = _sorted_unique(read)
    values, per_second = _as_units(read)
    breaks = pc.greater(
        pc.subtract(values[1:], values[:-1]),
        pa.scalar(int(max_gap * per_second), pa.int64()),
    )
    stretch = pc.cumulative_sum(
        pa.concat_arrays([pa.array([0], pa.int64()), breaks.cast(pa.int64())])
    )
    first_stretch = pc.take(
        stretch, pc.index_in(table["firstSeen"].chunk(0), value_set=read)
    )
    last_stretch = pc.take(
        stretch, pc.index_in(table["lastSeen"].chunk(0), value_set=read)
    )
    adjacent = pc.and_(
        adjacent, pc.equal(first_stretch[1:], last_stretch[:-1])
    )

    # A row continues the previous run if it has the same entity and content,
    # at the next fetch (a missing hash never matches)
    continues = pc.fill_null(
        pc.and_(
            pc.and_(same_entity, pc.equal(content[1:], content[:-1])),
            adjacent,
        ),
        False,
    )
    new_run = pa.concat_arrays([pa.array([True]), pc.invert(continues)])
    run = pc.cumulative_sum(pc.cast(new_run, pa.int64()))

    runs = (
        pa.table(
            {
                "run": run,
                "row": pa.array(range(table.num_rows), pa.int64()),
                "first": table["firstSeen"],
                "last": table["lastSeen"],
            }
        )
        .group_by("run", use_threads=False)
        .aggregate([("row", "min"), ("first", "min"), ("last", "max")])
    )
    result = table.take(runs["row_min"])
    result = result.set_column(
        result.schema.get_field_index("firstSeen"), "firstSeen", runs["first_min"]
    )
    result = result.set_column(
        result.schema.get_field_index("lastSeen"), "lastSeen", runs["last_max"]
    )
    if trip_keys:
        # Back to the order callers rely on (streaming compaction)
        result = result.take(
            pc.sort_indices(
                result,
                sort_keys=[("entityId", "ascending"), ("firstSeen", "ascending")],
            )
        )
    return result


def _sorted_unique(arrays) -> pa.Array:
    values = pc.unique(pa.chunked_array(arrays))
    return pc.drop_null(pc.take(values, pc.sort_indices(values)))
