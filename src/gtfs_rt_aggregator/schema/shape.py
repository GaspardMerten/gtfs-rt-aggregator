import pyarrow as pa

shape_schema = pa.schema(
    [
        pa.field("entityId", pa.string(), nullable=False),
        pa.field("fetchTime", pa.uint64(), nullable=False),
        # Timestamp from the feed header, and static version current at fetch time
        pa.field("feedTimestamp", pa.uint64(), nullable=True),
        pa.field("staticVersion", pa.string(), nullable=True),
        # Hash of the entity, to skip unchanged fetches and deduplicate rows
        pa.field("contentHash", pa.string(), nullable=True),
        pa.field("shapeId", pa.string(), nullable=True),
        pa.field("encodedPolyline", pa.string(), nullable=True),
    ]
)
