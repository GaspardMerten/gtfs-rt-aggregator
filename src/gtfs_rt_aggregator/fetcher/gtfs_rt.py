from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any

import pandas as pd
import pytz
import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

from ..utils import setup_logger

# Constants for GTFS-RT entity types and keys in the dictionary extracted from the feed
VEHICLE_POSITIONS = "VehiclePosition", 'vehicle'
TRIP_UPDATE = "TripUpdate", 'tripUpdate'
ALERT = "Alert", 'alert'
TRIP_MODIFICATIONS = "TripModifications", 'tripModifications'

SERVICE_TYPES = [VEHICLE_POSITIONS, TRIP_UPDATE, ALERT, TRIP_MODIFICATIONS]


class GtfsRtFetcher:
    """Class for fetching and parsing GTFS-RT data."""

    logger = setup_logger(f"{__name__}.GtfsRtFetcher")

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
            entities = feed_dict.get('entity', [])
            logger.debug(f"Found {len(entities)} entities in feed")

            # Group by entity type
            result = defaultdict(list)

            for entity in entities:
                entity_id = entity.get('id')

                for (service_name, service_key) in [VEHICLE_POSITIONS, TRIP_UPDATE, ALERT, TRIP_MODIFICATIONS]:
                    if service_key in entity:
                        result[service_name].append({
                            'entity_id': entity_id,
                            **entity[service_key]
                        })

            # Log counts by service type
            for service_name, entities_list in result.items():
                logger.debug(f"Found {len(entities_list)} entities of type {service_name}")

            return result
        except Exception as e:
            logger.error(f"Error parsing GTFS-RT feed: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def insert_fetch_time(entities: List[Dict[str, Any]], fetch_time: datetime) -> List[Dict[str, Any]]:
        """
        Add fetch time to entities.

        @param entities: List of entities
        @param fetch_time: Fetch time
        @return List of entities with fetch time added
        """
        logger = GtfsRtFetcher.logger
        logger.debug(f"Adding fetch time {fetch_time.isoformat()} to {len(entities)} entities")

        result = []
        for entity in entities:
            entity_copy = entity.copy()
            entity_copy['fetch_time'] = int(fetch_time.timestamp())
            result.append(entity_copy)
        return result

    @staticmethod
    def normalize_and_convert_to_df(entities: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convert entities to a DataFrame.

        @param entities: List of entities
        @return DataFrame
        """
        logger = GtfsRtFetcher.logger

        if not entities:
            logger.debug("No entities to convert to DataFrame")
            return pd.DataFrame()

        logger.debug(f"Converting {len(entities)} entities to DataFrame")

        try:
            # Normalize the JSON structure
            df = pd.json_normalize(
                entities,
                sep='_',
                errors='ignore'
            )

            # Order columns alphabetically
            df = df.reindex(sorted(df.columns), axis=1)

            return df
        except Exception as e:
            logger.error(f"Error converting entities to DataFrame: {str(e)}", exc_info=True)
            raise

    @classmethod
    def fetch_and_parse(cls, url: str, service_types: List[str], timezone: str) -> Dict[str, pd.DataFrame]:
        """
        Fetch and parse GTFS-RT data.

        @param url: URL of the GTFS-RT feed
        @param service_types: List of service types to fetch
        @param timezone: Timezone of the provider
        @return Dictionary with service types as keys and DataFrames as values
        """
        logger = cls.logger
        logger.info(f"Fetching and parsing GTFS-RT data from {url} for service types {service_types}")

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
                    entities_with_time = cls.insert_fetch_time(parsed_data[service_type], fetch_time)

                    # Convert to DataFrame
                    df = cls.normalize_and_convert_to_df(entities_with_time)

                    if not df.empty:
                        logger.info(f"Successfully processed {len(df)} records for service type {service_type}")
                    else:
                        logger.info(f"No data found for service type {service_type}")

                    result[service_type] = df
                else:
                    logger.warning(f"Service type {service_type} not found in feed")
                    result[service_type] = pd.DataFrame()

            return result

        except Exception as e:
            logger.error(f"Error fetching or parsing feed from {url}: {str(e)}", exc_info=True)
            # Return empty DataFrames for requested service types
            return {service_type: pd.DataFrame() for service_type in service_types}
