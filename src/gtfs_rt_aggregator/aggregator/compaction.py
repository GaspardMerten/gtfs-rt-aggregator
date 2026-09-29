"""
Merge sorted Parquet files into one sorted file without loading them whole.

Each input is sorted on its own (one file in memory at a time), then a batched
k-way merge takes, from every input's current batch, the rows up to the
smallest "last key" among those batches: all rows up to that key are then
known, sorted and written. Memory stays around one batch per input.
"""

import os
import tempfile
from typing import Iterator, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .dedup import deduplicate

BATCH_ROWS = 65_536


def _key_le(table: pa.Table, keys: Sequence[str], bound: Tuple) -> pa.Array:
    """Rows whose key (tuple of columns) is <= bound, lexicographically."""
    result = None
    # From the last key to the first: (k1 < b1) | (k1 == b1 & (...))
    for index in reversed(range(len(keys))):
        column = table[keys[index]]
        value = pa.scalar(bound[index], type=column.type)
        if result is None:
            result = pc.less_equal(column, value)
        else:
            result = pc.or_(
                pc.less(column, value), pc.and_(pc.equal(column, value), result)
            )
    return pc.fill_null(result, True)


def _last_key(table: pa.Table, keys: Sequence[str]) -> Tuple:
    return tuple(table[key][table.num_rows - 1].as_py() for key in keys)


def _sort(table: pa.Table, keys: Sequence[str]) -> pa.Table:
    return table.take(
        pc.sort_indices(table, sort_keys=[(k, "ascending") for k in keys])
    )


class _Input:
    """Batches of one sorted Parquet file, cast to the output schema."""

    def __init__(self, path: str, schema: pa.Schema, batch_rows: int):
        self.batches = pq.ParquetFile(path).iter_batches(batch_size=batch_rows)
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
                return
            self.current = align(pa.Table.from_batches([batch]), self.schema)


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


def merge_sorted(
    paths: List[str],
    keys: Sequence[str],
    schema: pa.Schema,
    batch_rows: int = BATCH_ROWS,
) -> Iterator[pa.Table]:
    """Yield the rows of sorted files, in order, as tables of at most a few batches."""
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
            yield _sort(chunk, keys)


def compact_files(
    local_paths: List[str],
    presorted: List[bool],
    keys: Sequence[str],
    output_path: str,
    deduplicate_rows: bool = False,
    times: Optional[pa.Array] = None,
    batch_rows: int = BATCH_ROWS,
    compression: str = "brotli",
) -> int:
    """
    Merge Parquet files into one file sorted by keys. Files not presorted are
    sorted first, one at a time. With deduplicate_rows, keys must start with
    entityId then firstSeen, and times holds every fetch time of the day (for
    gap detection). Returns the number of rows written.
    """
    schema = pa.unify_schemas(
        [pq.read_schema(p) for p in local_paths], promote_options="permissive"
    )
    with tempfile.TemporaryDirectory(dir=os.path.dirname(output_path)) as tmp:
        sorted_paths = []
        for index, (path, is_sorted) in enumerate(zip(local_paths, presorted)):
            if is_sorted:
                sorted_paths.append(path)
                continue
            table = align(pq.read_table(path), schema)
            sorted_path = os.path.join(tmp, f"{index}.parquet")
            pq.write_table(_sort(table, keys), sorted_path, compression="lz4")
            del table
            sorted_paths.append(sorted_path)

        rows = 0
        carry: Optional[pa.Table] = None
        if keys:
            chunks = merge_sorted(sorted_paths, keys, schema, batch_rows)
        else:
            # Nothing to sort by: files one after the other
            chunks = (
                align(pa.Table.from_batches([batch]), schema)
                for path in sorted_paths
                for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_rows)
            )
        with pq.ParquetWriter(output_path, schema, compression=compression) as writer:
            for chunk in chunks:
                if deduplicate_rows:
                    if carry is not None:
                        chunk = pa.concat_tables([carry, chunk])
                    chunk = align(deduplicate(chunk, times), schema)
                    # The last entity may continue in the next chunk: hold it back
                    last = chunk["entityId"][chunk.num_rows - 1]
                    held = pc.equal(chunk["entityId"], last)
                    carry = chunk.filter(held)
                    chunk = chunk.filter(pc.invert(held))
                if chunk.num_rows:
                    writer.write_table(chunk)
                    rows += chunk.num_rows
            if carry is not None and carry.num_rows:
                writer.write_table(carry)
                rows += carry.num_rows
    return rows
