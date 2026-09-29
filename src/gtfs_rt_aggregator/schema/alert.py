import pyarrow as pa

from ..schema.trip_update import trip_descriptor_type

# TimeRange
time_range_type = pa.struct(
    [
        pa.field("start", pa.int64(), nullable=True),
        pa.field("end", pa.int64(), nullable=True),
    ]
)

# EntitySelector uses the same TripDescriptor as in TripUpdate
entity_selector_type = pa.struct(
    [
        pa.field("agencyId", pa.string(), nullable=True),
        pa.field("routeId", pa.string(), nullable=True),
        pa.field("routeType", pa.int32(), nullable=True),
        pa.field("trip", trip_descriptor_type, nullable=True),
        pa.field("stopId", pa.string(), nullable=True),
        pa.field("directionId", pa.int64(), nullable=True),
    ]
)

# TranslatedString.Translation
translation_type = pa.struct(
    [
        pa.field("text", pa.string(), nullable=False),
        pa.field("language", pa.string(), nullable=True),
    ]
)

translated_string_type = pa.struct(
    [
        pa.field("translation", pa.list_(translation_type), nullable=True),
    ]
)

# TranslatedImage.LocalizedImage
localized_image_type = pa.struct(
    [
        pa.field("url", pa.string(), nullable=False),
        pa.field("mediaType", pa.string(), nullable=False),
        pa.field("language", pa.string(), nullable=True),
    ]
)

translated_image_type = pa.struct(
    [
        pa.field("localizedImage", pa.list_(localized_image_type), nullable=True),
    ]
)

alert_schema = pa.schema(
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
        pa.field("activePeriod", pa.list_(time_range_type), nullable=True),
        pa.field("informedEntity", pa.list_(entity_selector_type), nullable=True),
        # cause is an enum => int32
        pa.field("cause", pa.string(), nullable=True),
        # effect is an enum => int32
        pa.field("effect", pa.string(), nullable=True),
        pa.field("url", translated_string_type, nullable=True),
        pa.field("headerText", translated_string_type, nullable=True),
        pa.field("descriptionText", translated_string_type, nullable=True),
        pa.field("ttsHeaderText", translated_string_type, nullable=True),
        pa.field("ttsDescriptionText", translated_string_type, nullable=True),
        # severity_level is an enum => int32
        # Enum name (e.g. WARNING), like the other enums
        pa.field("severityLevel", pa.string(), nullable=True),
        pa.field("image", translated_image_type, nullable=True),
        pa.field("imageAlternativeText", translated_string_type, nullable=True),
        pa.field("causeDetail", translated_string_type, nullable=True),
        pa.field("effectDetail", translated_string_type, nullable=True),
    ]
)
