import pyarrow as pa

# StopSelector
stop_selector_type = pa.struct(
    [
        pa.field("stopSequence", pa.uint32(), nullable=True),
        pa.field("stopId", pa.string(), nullable=True),
    ]
)

# ReplacementStop
replacement_stop_type = pa.struct(
    [
        pa.field("travelTimeToStop", pa.int32(), nullable=True),
        pa.field("stopId", pa.string(), nullable=True),
    ]
)

# Modification
modification_type = pa.struct(
    [
        pa.field("startStopSelector", stop_selector_type, nullable=True),
        pa.field("endStopSelector", stop_selector_type, nullable=True),
        pa.field("propagatedModificationDelay", pa.int32(), nullable=True),
        pa.field("replacementStops", pa.list_(replacement_stop_type), nullable=True),
        pa.field("serviceAlertId", pa.string(), nullable=True),
        pa.field("lastModifiedTime", pa.uint64(), nullable=True),
    ]
)

# SelectedTrips
selected_trips_type = pa.struct(
    [
        pa.field("tripIds", pa.list_(pa.string()), nullable=True),
        pa.field("shapeId", pa.string(), nullable=True),
    ]
)

trip_modifications_schema = pa.schema(
    [
        pa.field("entityId", pa.string(), nullable=False),
        pa.field("fetchTime", pa.uint64(), nullable=False),
        pa.field("selectedTrips", pa.list_(selected_trips_type), nullable=True),
        pa.field("startTimes", pa.list_(pa.string()), nullable=True),
        pa.field("serviceDates", pa.list_(pa.string()), nullable=True),
        pa.field("modifications", pa.list_(modification_type), nullable=True),
    ]
)
