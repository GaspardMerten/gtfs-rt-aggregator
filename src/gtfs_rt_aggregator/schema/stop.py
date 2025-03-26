import pyarrow as pa

from ..schema.alert import translated_string_type

stop_schema = pa.schema(
    [
        pa.field("entityId", pa.string(), nullable=False),
        pa.field("fetchTime", pa.uint64(), nullable=False),
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
