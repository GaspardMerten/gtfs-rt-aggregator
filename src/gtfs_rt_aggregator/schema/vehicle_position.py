import pyarrow as pa

from ..schema.trip_update import (
    trip_descriptor_type,
    vehicle_descriptor_type,
)

# Position
position_type = pa.struct(
    [
        pa.field("latitude", pa.float32(), nullable=False),
        pa.field("longitude", pa.float32(), nullable=False),
        pa.field("bearing", pa.float32(), nullable=True),
        pa.field("odometer", pa.float64(), nullable=True),
        pa.field("speed", pa.float32(), nullable=True),
    ]
)

# CarriageDetails
carriage_details_type = pa.struct(
    [
        pa.field("id", pa.string(), nullable=True),
        pa.field("label", pa.string(), nullable=True),
        pa.field("occupancyStatus", pa.string(), nullable=True),
        pa.field("occupancyPercentage", pa.int32(), nullable=True),
        pa.field("carriageSequence", pa.uint32(), nullable=True),
    ]
)

vehicle_position_schema = pa.schema(
    [
        pa.field("entityId", pa.string(), nullable=False),
        pa.field("fetchTime", pa.uint64(), nullable=False),
        pa.field(
            "trip", trip_descriptor_type, nullable=True
        ),  # same TripDescriptor from above
        pa.field(
            "vehicle", vehicle_descriptor_type, nullable=True
        ),  # same VehicleDescriptor from above
        pa.field("position", position_type, nullable=True),
        pa.field("currentStopSequence", pa.uint32(), nullable=True),
        pa.field("stopId", pa.string(), nullable=True),
        pa.field("currentStatus", pa.string(), nullable=True),
        pa.field("timestamp", pa.uint64(), nullable=True),
        pa.field("congestionLevel", pa.string(), nullable=True),
        pa.field("occupancyStatus", pa.string(), nullable=True),
        pa.field("occupancyPercentage", pa.uint32(), nullable=True),
        pa.field(
            "multiCarriageDetails", pa.list_(carriage_details_type), nullable=True
        ),
    ]
)
