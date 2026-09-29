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
        pa.field("carriageSequence", pa.int64(), nullable=True),
    ]
)

vehicle_position_schema = pa.schema(
    [
        pa.field("entityId", pa.string(), nullable=False),
        pa.field("provider", pa.string(), nullable=True),
        # Local date of fetchTime, in the provider timezone
        pa.field("date", pa.date32(), nullable=True),
        pa.field("fetchTime", pa.timestamp("us", tz="UTC"), nullable=False),
        # Timestamp from the feed header, and static version current at fetch time
        pa.field("feedTimestamp", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("staticVersion", pa.string(), nullable=True),
        pa.field("feedId", pa.string(), nullable=True),
        # Hash of the entity, to skip unchanged fetches and deduplicate rows
        pa.field("contentHash", pa.string(), nullable=True),
        pa.field(
            "trip", trip_descriptor_type, nullable=True
        ),  # same TripDescriptor from above
        pa.field(
            "vehicle", vehicle_descriptor_type, nullable=True
        ),  # same VehicleDescriptor from above
        pa.field("position", position_type, nullable=True),
        pa.field("currentStopSequence", pa.int64(), nullable=True),
        pa.field("stopId", pa.string(), nullable=True),
        pa.field("currentStatus", pa.string(), nullable=True),
        pa.field("timestamp", pa.int64(), nullable=True),
        pa.field("congestionLevel", pa.string(), nullable=True),
        pa.field("occupancyStatus", pa.string(), nullable=True),
        pa.field("occupancyPercentage", pa.int64(), nullable=True),
        pa.field(
            "multiCarriageDetails", pa.list_(carriage_details_type), nullable=True
        ),
    ]
)
