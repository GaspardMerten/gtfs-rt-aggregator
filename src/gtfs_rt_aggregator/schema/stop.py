import pyarrow as pa

from ..schema.alert import translated_string_type

stop_schema = pa.schema(
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
        pa.field("stopId", pa.string(), nullable=True),
        pa.field("stopCode", translated_string_type, nullable=True),
        pa.field("stopName", translated_string_type, nullable=True),
        pa.field("ttsStopName", translated_string_type, nullable=True),
        pa.field("stopDesc", translated_string_type, nullable=True),
        pa.field("stopLat", pa.float32(), nullable=True),
        pa.field("stopLon", pa.float32(), nullable=True),
        pa.field("zoneId", pa.string(), nullable=True),
        pa.field("stopUrl", translated_string_type, nullable=True),
        pa.field("parentStation", pa.string(), nullable=True),
        pa.field("stopTimezone", pa.string(), nullable=True),
        pa.field("wheelchairBoarding", pa.string(), nullable=True),
        pa.field("levelId", pa.string(), nullable=True),
        pa.field("platformCode", translated_string_type, nullable=True),
    ]
)
