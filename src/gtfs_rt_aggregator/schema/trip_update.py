import pyarrow as pa

# StopTimeEvent
stop_time_event_type = pa.struct(
    [
        pa.field("delay", pa.int32(), nullable=True),
        pa.field("time", pa.int64(), nullable=True),
        pa.field("uncertainty", pa.int32(), nullable=True),
    ]
)

# StopTimeProperties
stop_time_properties_type = pa.struct(
    [
        pa.field("assignedStopId", pa.string(), nullable=True),
    ]
)

# StopTimeUpdate
stop_time_update_type = pa.struct(
    [
        pa.field("stopSequence", pa.int64(), nullable=True),
        pa.field("stopId", pa.string(), nullable=True),
        pa.field("arrival", stop_time_event_type, nullable=True),
        pa.field("departure", stop_time_event_type, nullable=True),
        pa.field("departureOccupancyStatus", pa.string(), nullable=True),
        pa.field("scheduleRelationship", pa.string(), nullable=True),
        pa.field("stopTimeProperties", stop_time_properties_type, nullable=True),
    ]
)

# TripProperties
trip_properties_type = pa.struct(
    [
        pa.field("tripId", pa.string(), nullable=True),
        pa.field("startDate", pa.string(), nullable=True),
        pa.field("startTime", pa.string(), nullable=True),
        pa.field("shapeId", pa.string(), nullable=True),
    ]
)

# ModifiedTripSelector
modified_trip_selector_type = pa.struct(
    [
        pa.field("modificationsId", pa.string(), nullable=True),
        pa.field("affectedTripId", pa.string(), nullable=True),
        pa.field("startTime", pa.string(), nullable=True),
        pa.field("startDate", pa.string(), nullable=True),
    ]
)

# TripDescriptor
trip_descriptor_type = pa.struct(
    [
        pa.field("tripId", pa.string(), nullable=True),
        pa.field("routeId", pa.string(), nullable=True),
        pa.field("directionId", pa.int64(), nullable=True),
        pa.field("startTime", pa.string(), nullable=True),
        pa.field("startDate", pa.string(), nullable=True),
        pa.field("scheduleRelationship", pa.string(), nullable=True),
        pa.field("modifiedTrip", modified_trip_selector_type, nullable=True),
    ]
)

# VehicleDescriptor
vehicle_descriptor_type = pa.struct(
    [
        pa.field("id", pa.string(), nullable=True),
        pa.field("label", pa.string(), nullable=True),
        pa.field("licensePlate", pa.string(), nullable=True),
        pa.field("wheelchairAccessible", pa.string(), nullable=True),
    ]
)

trip_update_schema = pa.schema(
    [
        # trip is required => nullable=False
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
        pa.field("trip", trip_descriptor_type, nullable=False),
        pa.field("vehicle", vehicle_descriptor_type, nullable=True),
        pa.field("stopTimeUpdate", pa.list_(stop_time_update_type), nullable=True),
        pa.field("timestamp", pa.int64(), nullable=True),
        pa.field("delay", pa.int32(), nullable=True),
        pa.field("tripProperties", trip_properties_type, nullable=True),
    ]
)
