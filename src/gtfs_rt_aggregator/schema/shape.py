import pyarrow as pa

shape_schema = pa.schema(
    [
        pa.field("entityId", pa.string(), nullable=False),
        pa.field("fetchTime", pa.uint64(), nullable=False),
        pa.field("shapeId", pa.string(), nullable=True),
        pa.field("encodedPolyline", pa.string(), nullable=True),
    ]
)
