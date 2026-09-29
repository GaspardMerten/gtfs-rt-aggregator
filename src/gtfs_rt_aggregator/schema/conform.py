"""Bring realtime tables written by earlier versions to the current schema."""

from datetime import timezone as dt_timezone
from functools import lru_cache
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
import pytz

# Metadata times: timestamps since 0.6.0, Unix seconds (uint64) before
TIME_COLUMNS = ("fetchTime", "feedTimestamp", "firstSeen", "lastSeen")
TIMESTAMP = pa.timestamp("us", tz="UTC")
_MAX_SECONDS = (2**63 - 1) // 1_000_000


@lru_cache(maxsize=None)
def stored_schema(service_type: str) -> pa.Schema:
    """Schema of the files of a service: flattened, with "." in names as "_"."""
    from ..fetcher.gtfs_rt import SERVICE_TYPE_TO_SCHEMA

    empty = SERVICE_TYPE_TO_SCHEMA[service_type].empty_table().flatten()
    return empty.rename_columns(
        [c.replace(".", "_") for c in empty.column_names]
    ).schema


def conform(
    table: pa.Table,
    service_type: Optional[str],
    provider: Optional[str] = None,
    timezone: Optional[pytz.BaseTzInfo] = None,
) -> pa.Table:
    """
    Cast a table read from storage to the current schema, so files from
    before 0.6.0 (unsigned integers, times as Unix seconds, no provider or
    date column) can be merged with new ones. Current tables are returned as
    they are; columns unknown to the schema are kept.
    """
    from ..fetcher.gtfs_rt import SERVICE_TYPE_TO_SCHEMA

    target = (
        stored_schema(service_type) if service_type in SERVICE_TYPE_TO_SCHEMA else None
    )
    for name in TIME_COLUMNS:
        if name in table.column_names and pa.types.is_integer(
            table.schema.field(name).type
        ):
            column = table[name]
            # Beyond what microseconds hold: nonsense, kept as null
            valid = pc.less_equal(column, pa.scalar(_MAX_SECONDS, column.type))
            seconds = pc.cast(
                pc.if_else(valid, column, pa.scalar(None, column.type)), pa.int64()
            )
            values = pc.cast(pc.cast(seconds, pa.timestamp("s", tz="UTC")), TIMESTAMP)
            table = table.set_column(table.schema.get_field_index(name), name, values)

    if target is not None:
        for field in target:
            if field.name in table.column_names:
                current = table.schema.field(field.name).type
                if current != field.type:
                    index = table.schema.get_field_index(field.name)
                    try:
                        values = pc.cast(table[field.name], field.type)
                    except pa.ArrowInvalid:
                        # e.g. an unsigned value over 2^63 somewhere in a nested
                        # column: such values are nonsense, don't fail the file
                        values = pc.cast(table[field.name], field.type, safe=False)
                    table = table.set_column(index, field.name, values)

    if "provider" not in table.column_names and provider is not None:
        table = table.append_column(
            "provider", pa.array([provider] * table.num_rows, pa.string())
        )
    if "date" not in table.column_names and "fetchTime" in table.column_names:
        tz = str(timezone) if timezone is not None else "UTC"
        local = pc.cast(table["fetchTime"], pa.timestamp("us", tz=tz))
        table = table.append_column(
            "date", pc.cast(pc.local_timestamp(local), pa.date32())
        )
    return table
