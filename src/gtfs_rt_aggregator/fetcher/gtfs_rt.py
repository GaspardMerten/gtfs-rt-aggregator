import hashlib
from collections import defaultdict
from datetime import datetime
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
        added = {"fetchTime": int(fetch_time.timestamp()), **(extra or {})}
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
            header_timestamp, parsed_data = cls.parse_feed_with_header(data)
            return cls.to_tables(
                parsed_data,
                service_types,
                fetch_time,
                {"feedTimestamp": header_timestamp},
            )
        except Exception as e:
            cls.logger.error(
                f"Error fetching or parsing feed from {url}: {str(e)}", exc_info=True
            )
            return {}
