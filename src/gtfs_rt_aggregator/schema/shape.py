import pyarrow as pa

shape_schema = pa.schema(
    [
        pa.field("entityId", pa.string(), nullable=False),
        pa.field("provider", pa.string(), nullable=True),
        # Local date of fetchTime, in the provider timezone
        pa.field("date", pa.date32(), nullable=True),
        pa.field("fetchTime", pa.timestamp("us", tz="UTC"), nullable=False),
        # Timestamp from the feed header, and static version current at fetch time
        pa.field("feedTimestamp", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("staticVersion", pa.string(), nullable=True),
        # Hash of the entity, to skip unchanged fetches and deduplicate rows
        pa.field("contentHash", pa.string(), nullable=True),
        pa.field("shapeId", pa.string(), nullable=True),
        pa.field("encodedPolyline", pa.string(), nullable=True),
    ]
)
