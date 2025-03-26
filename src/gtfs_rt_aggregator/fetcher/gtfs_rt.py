from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any

import pyarrow as pa
import pytz
import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

from ..schema.alert import alert_schema
from ..schema.shape import shape_schema
from ..schema.stop import stop_schema
from ..schema.trip_update import trip_update_schema
from ..schema.vehicle_position import vehicle_position_schema
from ..utils import setup_logger

VEHICLE_POSITIONS = "VehiclePosition", "vehicle", vehicle_position_schema
TRIP_UPDATE = "TripUpdate", "tripUpdate", trip_update_schema
ALERT = ("Alert", "alert", alert_schema)
SHAPE = "Shape", "shape", shape_schema
STOP = "Stop", "stop", stop_schema
TRIP_MODIFICATIONS = "TripModifications", "tripModifications", trip_update_schema

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
    def fetch_feed(url: str) -> bytes:
        """
        Fetch GTFS-RT feed from a URL.

        @param url: URL of the GTFS-RT feed
        @return Binary data of the feed
        @raises requests.RequestException: If the request fails
        """
        logger = GtfsRtFetcher.logger
        logger.debug(f"Fetching GTFS-RT feed from {url}")

        try:
            response = requests.get(url)
            response.raise_for_status()
            content_length = len(response.content)
            logger.debug(f"Successfully fetched {content_length} bytes from {url}")
            return response.content
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
        logger = GtfsRtFetcher.logger
        logger.debug(f"Parsing {len(data)} bytes of GTFS-RT feed data")

        try:
            # noinspection PyUnresolvedReferences
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(data)

            # Convert to dictionary
            feed_dict = MessageToDict(feed)

            # Extract entities
            entities = feed_dict.get("entity", [])
            logger.debug(f"Found {len(entities)} entities in feed")

            # Group by entity type
            result = defaultdict(list)

            for entity in entities:
                entity_id = entity.get("id")

                for service_name, service_key, schema in SERVICE_TYPES:
                    if service_key in entity:
                        result[service_name].append(
                            {
                                "entityId": entity_id,
                                **GtfsRtFetcher.convert_timestamp_to_int(
                                    entity[service_key], service_name
                                ),
                            }
                        )

            # Log counts by service type
            for service_name, entities_list in result.items():
                logger.debug(
                    f"Found {len(entities_list)} entities of type {service_name}"
                )

            return result
        except Exception as e:
            logger.error(f"Error parsing GTFS-RT feed: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def insert_fetch_time(
        entities: List[Dict[str, Any]], fetch_time: datetime
    ) -> List[Dict[str, Any]]:
        """
        Add fetch time to entities.

        @param entities: List of entities
        @param fetch_time: Fetch time
        @return List of entities with fetch time added
        """
        logger = GtfsRtFetcher.logger
        logger.debug(
            f"Adding fetch time {fetch_time.isoformat()} to {len(entities)} entities"
        )

        result = []
        for entity in entities:
            entity_copy = entity.copy()
            entity_copy["fetchTime"] = int(fetch_time.timestamp())
            result.append(entity_copy)
        return result

    @classmethod
    def fetch_and_parse(
        cls, url: str, service_types: List[str], timezone: str
    ) -> Dict[str, pa.Table]:
        """
        Fetch and parse GTFS-RT data.

        @param url: URL of the GTFS-RT feed
        @param service_types: List of service types to fetch
        @param timezone: Timezone of the provider
        @return Dictionary with service types as keys and DataFrames as values
        """
        logger = cls.logger
        logger.info(
            f"Fetching and parsing GTFS-RT data from {url} for service types {service_types}"
        )

        # Get timezone
        tz = pytz.timezone(timezone)

        # Fetch time
        fetch_time = datetime.now(tz)
        logger.debug(f"Fetch time: {fetch_time.isoformat()}")

        try:
            # Fetch feed
            logger.debug(f"Fetching feed from {url}")
            feed_data = cls.fetch_feed(url)

            # Parse feed
            logger.debug("Parsing feed data")
            parsed_data = cls.parse_feed(feed_data)

            # Filter and convert to DataFrames
            result = {}
            for service_type in service_types:
                if service_type in parsed_data:
                    logger.debug(f"Processing service type: {service_type}")

                    # Add fetch time
                    entities_with_time = cls.insert_fetch_time(
                        parsed_data[service_type], fetch_time
                    )
                    table = pa.Table.from_pylist(
                        entities_with_time,
                        schema=SERVICE_TYPE_TO_SCHEMA[service_type],
                    ).flatten()

                    table = table.rename_columns(
                        [col.replace(".", "_") for col in table.column_names]
                    )

                    if table.num_rows > 0:
                        logger.info(
                            f"Successfully processed {table.num_rows} records for service type {service_type}"
                        )
                    else:
                        logger.info(f"No data found for service type {service_type}")

                    result[service_type] = table
                else:
                    logger.warning(f"Service type {service_type} not found in feed")

            return result

        except Exception as e:
            logger.error(
                f"Error fetching or parsing feed from {url}: {str(e)}", exc_info=True
            )
            # Return empty DataFrames for requested service types
            return {}
