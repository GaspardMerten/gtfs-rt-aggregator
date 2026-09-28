import pyarrow as pa
import pyarrow.compute as pc


def deduplicate(table: pa.Table) -> pa.Table:
    """
    Merge consecutive rows of the same entity with the same content.

    Rows are compared on entityId and contentHash (a hash of the whole entity,
    set when fetching). Each run of identical consecutive rows of an entity
    becomes its first row, with firstSeen and lastSeen set to the first and
    last fetchTime of the run. Tables that are already deduplicated (they have
    firstSeen and lastSeen) can be merged again with new rows.

    Rows without contentHash (written before 0.5.0) are kept as they are.
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

    table = table.take(
        pc.sort_indices(
            table, sort_keys=[("entityId", "ascending"), ("firstSeen", "ascending")]
        )
    ).combine_chunks()
    entity = table["entityId"].chunk(0).cast(pa.string())
    # All-null hashes have the null type, which cannot be compared
    content = table["contentHash"].chunk(0).cast(pa.string())

    # A row continues the previous run if it has the same entity and content
    # (a missing hash never matches)
    continues = pc.fill_null(
        pc.and_(pc.equal(entity[1:], entity[:-1]), pc.equal(content[1:], content[:-1])),
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
    return result.set_column(
        result.schema.get_field_index("lastSeen"), "lastSeen", runs["last_max"]
    )
