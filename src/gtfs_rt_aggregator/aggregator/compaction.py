"""
Merge sorted Parquet files into one sorted file without loading them whole.

Every input is sorted on its own and written with small row groups, then a
batched k-way merge takes, from every input's current batch, the rows up to
the smallest "last key" among those batches: all rows up to that key are
known, sorted and written. Memory is bounded by ROW_BUDGET rows spread over the
inputs, merged at most MAX_FAN_IN at a time (more inputs are merged in rounds).
Nulls sort last, as in pyarrow's sort.
"""

import json
import os
import tempfile
from typing import Iterator, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .dedup import deduplicate

# Rows held by the merge at once, whatever the number of inputs
ROW_BUDGET = 400_000
MAX_FAN_IN = 8
MIN_BATCH_ROWS = 2_000
# Parquet metadata key recording the sort of a compacted file
SORT_KEYS_METADATA = b"gtfs_rt_aggregator.sort_keys"


def _key_value(value):
    """Tuple element for Python comparisons with nulls last."""
    return (value is None, value)


def _key_le(table: pa.Table, keys: Sequence[str], bound: Tuple) -> pa.Array:
    """Rows whose key is <= bound, lexicographically, nulls last."""
    result = None
    # From the last key to the first: (k1 < b1) | (k1 == b1 & (...))
    for index in reversed(range(len(keys))):
        column = table[keys[index]]
        bound_null, value = bound[index]
        if bound_null:
            less = pc.is_valid(column)  # any value < null
            equal = pc.is_null(column)
        else:
            scalar = pa.scalar(value, type=column.type)
            less = pc.fill_null(pc.less(column, scalar), False)
            equal = pc.fill_null(pc.equal(column, scalar), False)
        if result is None:
            result = pc.or_(less, equal)
        else:
            result = pc.or_(less, pc.and_(equal, result))
    return result


def _last_key(table: pa.Table, keys: Sequence[str]) -> Tuple:
    return tuple(_key_value(table[key][table.num_rows - 1].as_py()) for key in keys)


def sort_table(table: pa.Table, keys: Sequence[str]) -> pa.Table:
    if not keys:
        return table
    return table.take(
        pc.sort_indices(
            table, sort_keys=[(k, "ascending") for k in keys]
        )  # nulls sort last by default
    )


def align(table: pa.Table, schema: pa.Schema) -> pa.Table:
    """Give table exactly the columns of schema (missing ones as nulls)."""
    columns = []
    for field in schema:
        if field.name in table.column_names:
            column = table[field.name]
            columns.append(
                column if column.type == field.type else pc.cast(column, field.type)
            )
        else:
            columns.append(pa.nulls(table.num_rows, field.type))
    return pa.Table.from_arrays(columns, schema=schema)


def write_sorted(
    table: pa.Table, keys: Sequence[str], path: str, row_group_rows: int = 50_000
):
    """Write a table sorted by keys, in small row groups (read back in batches)."""
    pq.write_table(
        sort_table(table, keys), path, compression="lz4", row_group_size=row_group_rows
    )


class _Input:
    """Batches of one sorted Parquet file, cast to the output schema."""

    def __init__(self, path: str, schema: pa.Schema, batch_rows: int):
        # No read-ahead: a reader holds about one batch, not whole row groups
        self.file = pq.ParquetFile(path, pre_buffer=False, buffer_size=1 << 20)
        self.batches = self.file.iter_batches(batch_size=batch_rows)
        self.schema = schema
        self.current: Optional[pa.Table] = None
        self.done = False

    def fill(self):
        while (self.current is None or self.current.num_rows == 0) and not self.done:
            try:
                batch = next(self.batches)
            except StopIteration:
                self.done = True
                self.current = None
                self.file.close()
                return
            self.current = align(pa.Table.from_batches([batch]), self.schema)


def merge_sorted(
    paths: List[str], keys: Sequence[str], schema: pa.Schema, batch_rows: int
) -> Iterator[pa.Table]:
    """Yield the rows of sorted files, in order, a bounded number at a time."""
    inputs = [_Input(path, schema, batch_rows) for path in paths]
    for source in inputs:
        source.fill()
    while True:
        live = [s for s in inputs if s.current is not None and s.current.num_rows]
        if not live:
            return
        # Every row up to the smallest last key of the current batches is known
        bound = min(_last_key(s.current, keys) for s in live)
        parts = []
        for source in live:
            mask = _key_le(source.current, keys, bound)
            parts.append(source.current.filter(mask))
            source.current = source.current.filter(pc.invert(mask))
            source.fill()
        chunk = pa.concat_tables([p for p in parts if p.num_rows])
        if chunk.num_rows:
            yield sort_table(chunk, keys)


def _batch_rows(inputs: int) -> int:
    return max(MIN_BATCH_ROWS, ROW_BUDGET // max(1, inputs))


def compact_files(
    sorted_paths: List[str],
    keys: Sequence[str],
    output_path: str,
    deduplicate_rows: bool = False,
    times: Optional[pa.Array] = None,
    compression: str = "brotli",
) -> int:
    """
    Merge Parquet files, each already sorted by keys (see write_sorted), into
    one file sorted by keys. With deduplicate_rows, keys must start with
    entityId then firstSeen, and times holds every fetch time of the day (for
    gap detection). Returns the number of rows written.
    """
    schema = pa.unify_schemas(
        [pq.read_schema(p).remove_metadata() for p in sorted_paths],
        promote_options="permissive",
    )
    with tempfile.TemporaryDirectory(dir=os.path.dirname(output_path)) as tmp:
        # Too many inputs: merge them in rounds, MAX_FAN_IN at a time
        round_number = 0
        while keys and len(sorted_paths) > MAX_FAN_IN:
            merged = []
            for start in range(0, len(sorted_paths), MAX_FAN_IN):
                group = sorted_paths[start : start + MAX_FAN_IN]
                path = os.path.join(tmp, f"round{round_number}-{start}.parquet")
                _write(
                    merge_sorted(group, keys, schema, _batch_rows(len(group))),
                    path,
                    schema,
                    "lz4",
                    _batch_rows(MAX_FAN_IN),
                )
                merged.append(path)
            sorted_paths = merged
            round_number += 1

        batch_rows = _batch_rows(len(sorted_paths))
        if keys:
            chunks = merge_sorted(sorted_paths, keys, schema, batch_rows)
        else:
            # Nothing to sort by: files one after the other
            chunks = (
                align(pa.Table.from_batches([batch]), schema)
                for path in sorted_paths
                for batch in pq.ParquetFile(path, pre_buffer=False).iter_batches(
                    batch_size=batch_rows
                )
            )
        if deduplicate_rows:
            chunks = _deduplicated(chunks, times, schema)
        metadata = {SORT_KEYS_METADATA: json.dumps(list(keys)).encode()}
        return _write(chunks, output_path, schema.with_metadata(metadata), compression)


def _write(
    chunks, path: str, schema: pa.Schema, compression: str, row_group_rows=None
) -> int:
    rows = 0
    with pq.ParquetWriter(path, schema, compression=compression) as writer:
        for chunk in chunks:
            if chunk.num_rows:
                writer.write_table(align(chunk, schema), row_group_size=row_group_rows)
                rows += chunk.num_rows
    return rows


def _deduplicated(chunks, times, schema) -> Iterator[pa.Table]:
    """Deduplicate sorted chunks; the last entity of a chunk may continue in the next."""
    carry: Optional[pa.Table] = None
    for chunk in chunks:
        if carry is not None:
            chunk = pa.concat_tables([carry, chunk])
        chunk = align(deduplicate(chunk, times), schema)
        last = chunk["entityId"][chunk.num_rows - 1]
        held = pc.equal(chunk["entityId"], last)
        carry = chunk.filter(held)
        yield chunk.filter(pc.invert(held))
    if carry is not None:
        yield carry


def sorted_by(path: str) -> Optional[List[str]]:
    """Keys a compacted file was sorted by (None if unknown, e.g. before 0.6.0)."""
    metadata = pq.read_schema(path).metadata or {}
    value = metadata.get(SORT_KEYS_METADATA)
    return json.loads(value) if value else None
