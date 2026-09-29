import hashlib
from collections import defaultdict
from datetime import datetime, timezone as dt_timezone
from typing import Dict, Iterable, List, Any, Optional, Tuple

import pyarrow as pa
import pytz
import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

from ..schema.alert import alert_schema
from ..schema.shape import shape_schema
from ..schema.stop import stop_schema
from ..schema.trip_modifications import trip_modifications_schema
from ..schema.trip_update import trip_update_schema
from ..schema.vehicle_position import vehicle_position_schema
from ..fetcher.arrow_builder import TableBuilder
from ..utils import setup_logger
from ..utils.http import get_bytes

VEHICLE_POSITIONS = "VehiclePosition", "vehicle", vehicle_position_schema
TRIP_UPDATE = "TripUpdate", "tripUpdate", trip_update_schema
ALERT = ("Alert", "alert", alert_schema)
SHAPE = "Shape", "shape", shape_schema
STOP = "Stop", "stop", stop_schema
TRIP_MODIFICATIONS = (
    "TripModifications",
    "tripModifications",
    trip_modifications_schema,
)

SERVICE_TYPES = [VEHICLE_POSITIONS, TRIP_UPDATE, ALERT, TRIP_MODIFICATIONS, SHAPE, STOP]
SERVICE_TYPE_TO_SCHEMA = {x[0]: x[2] for x in SERVICE_TYPES}
# FeedEntity field holding each service type
ENTITY_FIELDS = {
    "VehiclePosition": "vehicle",
    "TripUpdate": "trip_update",
    "Alert": "alert",
    "TripModifications": "trip_modifications",
    "Shape": "shape",
    "Stop": "stop",
}
# Columns not read from the entity's message
ROW_FIELDS = (
    "entityId",
    "contentHash",
    "provider",
    "date",
    "fetchTime",
    "feedTimestamp",
    "staticVersion",
    "feedId",
)


def row_metadata(
    provider: Optional[str],
    fetch_time: datetime,
    header_timestamp: Optional[int],
    static_version: Optional[str],
    feed_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Columns added to every realtime row besides fetchTime.

    @param provider: Provider name
    @param fetch_time: Fetch time, aware, in the provider timezone (gives the date)
    @param header_timestamp: Timestamp of the feed header (Unix time), if any
    @param static_version: Static version current at fetch time, if any
    @param feed_id: Short id of the realtime feed (see feed_hash): tells apart
        the feeds of a provider that give the same service
    """
    try:
        feed_timestamp = (
            datetime.fromtimestamp(header_timestamp, dt_timezone.utc)
            if header_timestamp
            else None
        )
    except (OverflowError, OSError, ValueError):
        feed_timestamp = None  # a header time out of any sensible range
    return {
        "provider": provider,
        "date": fetch_time.date(),
        "feedTimestamp": feed_timestamp,
        "staticVersion": static_version,
        "feedId": feed_id,
    }


class GtfsRtFetcher:
    """Class for fetching and parsing GTFS-RT data."""

    logger = setup_logger(f"{__name__}.GtfsRtFetcher")

    @staticmethod
    def parse_int(value):
        if value is None:
            return None

        try:
            return int(value)
        except ValueError:
            return None

    @staticmethod
    def convert_timestamp_to_int(entity, service):
        if service == VEHICLE_POSITIONS[0]:
            entity["timestamp"] = GtfsRtFetcher.parse_int(entity.get("timestamp"))
        elif service == TRIP_UPDATE[0]:
            entity["timestamp"] = GtfsRtFetcher.parse_int(entity.get("timestamp"))
            for stop_time_update in entity.get("stopTimeUpdate", []):
                if "arrival" in stop_time_update:
                    stop_time_update["arrival"]["time"] = GtfsRtFetcher.parse_int(
                        stop_time_update["arrival"].get("time")
                    )
                if "departure" in stop_time_update:
                    stop_time_update["departure"]["time"] = GtfsRtFetcher.parse_int(
                        stop_time_update["departure"].get("time")
                    )
        elif service == TRIP_MODIFICATIONS[0]:
            for modification in entity.get("modifications", []):
                modification["lastModifiedTime"] = GtfsRtFetcher.parse_int(
                    modification.get("lastModifiedTime")
                )
        elif service == ALERT[0]:
            if "activePeriod" in entity:
                for active_period in entity["activePeriod"]:
                    active_period["start"] = GtfsRtFetcher.parse_int(
                        active_period.get("start")
                    )
                    active_period["end"] = GtfsRtFetcher.parse_int(
                        active_period.get("end")
                    )

        return entity

    @staticmethod
    def fetch_feed(
        url: str, headers: Optional[Dict[str, str]] = None, retries: int = 3
    ) -> bytes:
        """
        Fetch GTFS-RT feed from a URL.

        @param url: URL of the GTFS-RT feed
        @param headers: HTTP headers to send (e.g. an API key)
        @param retries: Retries on connection errors, timeouts and 429/5xx
        @return Binary data of the feed
        @raises requests.RequestException: If the request still fails after retries
        """
        logger = GtfsRtFetcher.logger
        logger.debug(f"Fetching GTFS-RT feed from {url}")
        try:
            data = get_bytes(url, headers, retries, logger)
            logger.debug(f"Successfully fetched {len(data)} bytes from {url}")
            return data
        except requests.RequestException as e:
            logger.error(f"Failed to fetch feed from {url}: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def parse_feed(data: bytes) -> Dict[str, List[Dict[str, Any]]]:
        """
        Parse GTFS-RT feed data.

        @param data: Binary data of the feed
        @return Dictionary with entity types as keys and lists of entities as values
        """
        return GtfsRtFetcher.parse_feed_with_header(data)[1]

    @staticmethod
    def parse_feed_with_header(
        data: bytes,
    ) -> Tuple[Optional[int], Dict[str, List[Dict[str, Any]]]]:
        """
        Parse GTFS-RT feed data.

        @param data: Binary data of the feed
        @return The header timestamp (Unix time, None if absent) and the entities
            by entity type
        """
        feed = GtfsRtFetcher.parse_message(data)
        return feed.header.timestamp or None, GtfsRtFetcher.entities_by_service(
            feed.entity
        )

    @staticmethod
    def parse_message(data: bytes):
        """Parse GTFS-RT bytes into a FeedMessage."""
        # noinspection PyUnresolvedReferences
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data)
        GtfsRtFetcher.logger.debug(
            f"Parsed {len(data)} bytes, {len(feed.entity)} entities"
        )
        return feed

    @staticmethod
    def entity_hash(entity) -> str:
        """Hash of an entity's content, to tell unchanged entities apart."""
        return hashlib.blake2b(
            entity.SerializeToString(deterministic=True), digest_size=8
        ).hexdigest()

    @staticmethod
    def entities_by_service(entities: Iterable) -> Dict[str, List[Dict[str, Any]]]:
        """
        Convert FeedEntity messages to dicts, grouped by service type.

        Each dict gets the entity id (entityId) and a hash of the entity
        (contentHash), used to skip unchanged fetches and deduplicate rows.
        """
        result = defaultdict(list)
        for entity in entities:
            entity_dict = MessageToDict(entity)
            content_hash = GtfsRtFetcher.entity_hash(entity)
            for service_name, service_key, schema in SERVICE_TYPES:
                if service_key in entity_dict:
                    result[service_name].append(
                        {
                            "entityId": entity_dict.get("id"),
                            "contentHash": content_hash,
                            **GtfsRtFetcher.convert_timestamp_to_int(
                                entity_dict[service_key], service_name
                            ),
                        }
                    )
        return result

    @staticmethod
    def insert_fetch_time(
        entities: List[Dict[str, Any]],
        fetch_time: datetime,
        extra: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Add fetch time, and any extra columns, to entities.

        @param entities: List of entities
        @param fetch_time: Fetch time
        @param extra: Other values to add to every entity (e.g. feedTimestamp)
        @return List of entities with fetch time added
        """
        added = {"fetchTime": fetch_time.astimezone(dt_timezone.utc), **(extra or {})}
        return [{**entity, **added} for entity in entities]

    @classmethod
    def to_tables(
        cls,
        parsed_data: Dict[str, List[Dict[str, Any]]],
        service_types: List[str],
        fetch_time: datetime,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, pa.Table]:
        """
        Build one flattened table per requested service type.

        @param parsed_data: Entities by service type, from parse_feed
        @param service_types: Service types to keep
        @param fetch_time: Fetch time, stored in every row
        @param extra: Other values stored in every row (e.g. feedTimestamp)
        @return Tables by service type (missing service types are left out)
        """
        result = {}
        for service_type in service_types:
            if service_type not in parsed_data:
                cls.logger.warning(f"Service type {service_type} not found in feed")
                continue
            table = pa.Table.from_pylist(
                cls.insert_fetch_time(parsed_data[service_type], fetch_time, extra),
                schema=SERVICE_TYPE_TO_SCHEMA[service_type],
            ).flatten()
            result[service_type] = table.rename_columns(
                [col.replace(".", "_") for col in table.column_names]
            )
            cls.logger.info(
                f"Processed {table.num_rows} records for service type {service_type}"
            )
        return result

    @classmethod
    def build_tables(
        cls,
        entities: List,
        hashes: List[str],
        service_types: List[str],
        fetch_time: datetime,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, pa.Table]:
        """
        Build one flattened table per requested service type, straight from
        the protobuf entities (no dicts: see arrow_builder).

        @param entities: FeedEntity messages
        @param hashes: entity_hash() of each entity
        @param service_types: Service types to keep
        @param fetch_time: Fetch time, stored in every row
        @param extra: Other values stored in every row (see row_metadata)
        @return Tables by service type (service types absent from the feed are left out)
        """
        constants = {
            "fetchTime": fetch_time.astimezone(dt_timezone.utc),
            **(extra or {}),
        }
        builders = {}
        for service_type in service_types:
            field = ENTITY_FIELDS[service_type]
            builders[field] = (
                service_type,
                TableBuilder(
                    SERVICE_TYPE_TO_SCHEMA[service_type],
                    gtfs_realtime_pb2.FeedEntity.DESCRIPTOR.fields_by_name[
                        field
                    ].message_type,
                    ROW_FIELDS,
                ),
            )
        seen = set()
        for entity, content_hash in zip(entities, hashes):
            for field, (service_type, builder) in builders.items():
                if entity.HasField(field):
                    builder.add(entity.id, content_hash, getattr(entity, field))
                    seen.add(service_type)
        result = {}
        for service_type, builder in builders.values():
            if service_type not in seen:
                cls.logger.warning(f"Service type {service_type} not found in feed")
                continue
            table = builder.finish(constants).flatten()
            result[service_type] = table.rename_columns(
                [col.replace(".", "_") for col in table.column_names]
            )
            cls.logger.info(
                f"Processed {table.num_rows} records for service type {service_type}"
            )
        return result

    @classmethod
    def fetch_and_parse(
        cls,
        url: str,
        service_types: List[str],
        timezone: str,
        headers: Optional[Dict[str, str]] = None,
        retries: int = 3,
    ) -> Dict[str, pa.Table]:
        """
        Fetch and parse GTFS-RT data.

        @param url: URL of the GTFS-RT feed
        @param service_types: List of service types to fetch
        @param timezone: Timezone of the provider
        @param headers: HTTP headers to send (e.g. an API key)
        @param retries: Retries on connection errors, timeouts and 429/5xx
        @return Dictionary with service types as keys and tables as values
        """
        fetch_time = datetime.now(pytz.timezone(timezone))
        try:
            data = cls.fetch_feed(url, headers, retries)
            message = cls.parse_message(data)
            entities = list(message.entity)
            return cls.build_tables(
                entities,
                [cls.entity_hash(e) for e in entities],
                service_types,
                fetch_time,
                row_metadata(None, fetch_time, message.header.timestamp or None, None),
            )
        except Exception as e:
            cls.logger.error(
                f"Error fetching or parsing feed from {url}: {str(e)}", exc_info=True
            )
            return {}
