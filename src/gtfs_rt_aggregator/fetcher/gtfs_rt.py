import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any

import pandas as pd
import pytz
import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

from gtfs_rt_aggregator.utils.log_helper import setup_logger

# Constants for GTFS-RT entity types and keys in the dictionary extracted from the feed
VEHICLE_POSITIONS = "VehiclePosition", "vehicle"
TRIP_UPDATE = "TripUpdate", "tripUpdate"
ALERT = "Alert", "alert"
TRIP_MODIFICATIONS = "TripModifications", "tripModifications"

SERVICE_TYPES = [VEHICLE_POSITIONS, TRIP_UPDATE, ALERT, TRIP_MODIFICATIONS]


class GtfsRtError(Exception):
    """Base exception for GTFS-RT related errors."""

    pass


class GtfsRtFetchError(GtfsRtError):
    """Exception raised when fetching GTFS-RT data fails."""

    pass


class GtfsRtParseError(GtfsRtError):
    """Exception raised when parsing GTFS-RT data fails."""

    pass


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
            entities = feed_dict.get("entity", [])
            logger.debug(f"Found {len(entities)} entities in feed")

            # Group by entity type
            result = defaultdict(list)

            for entity in entities:
                entity_id = entity.get("id")

                for service_name, service_key in [
                    VEHICLE_POSITIONS,
                    TRIP_UPDATE,
                    ALERT,
                    TRIP_MODIFICATIONS,
                ]:
                    if service_key in entity:
                        result[service_name].append(
                            {"entity_id": entity_id, **entity[service_key]}
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
            entity_copy["fetch_time"] = fetch_time
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
            df = pd.json_normalize(entities, sep="_", errors="ignore")

            # Order columns alphabetically
            df = df.reindex(sorted(df.columns), axis=1)

            return df
        except Exception as e:
            logger.error(
                f"Error converting entities to DataFrame: {str(e)}", exc_info=True
            )
            raise

    @classmethod
    def fetch_and_parse(
        cls,
        url: str,
        service_types: List[str],
        timezone: str,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch and parse GTFS-RT data from a URL.

        @param url: URL to fetch data from
        @param service_types: List of service types to fetch
        @param timezone: Timezone to use for timestamps
        @param max_retries: Maximum number of retries for failed requests
        @param retry_delay: Delay between retries in seconds
        @return Dictionary with service types as keys and pandas dataframes as values
        @raises GtfsRtFetchError: If fetching the data fails after retries
        @raises GtfsRtParseError: If parsing the data fails
        """
        logger = cls.logger
        logger.info(
            f"Fetching and parsing GTFS-RT data from {url} for service types {service_types}"
        )

        # Initialize result dictionary
        result = {}

        # Fetch the data
        logger.debug(f"Fetching data from {url}")
        feed = None

        # Retry logic for network errors
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()  # Raise error for bad status codes

                # Parse the protobuf message
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(response.content)
                break
            except requests.RequestException as e:
                logger.warning(
                    f"Request failed (attempt {attempt+1}/{max_retries}): {str(e)}"
                )
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    raise GtfsRtFetchError(
                        f"Failed to fetch data from {url} after {max_retries} attempts: {str(e)}"
                    ) from e
            except Exception as e:
                raise GtfsRtFetchError(
                    f"Unknown error fetching data from {url}: {str(e)}"
                ) from e

        if feed is None:
            raise GtfsRtFetchError(f"Failed to fetch data from {url}")

        # Get timezone
        tz = pytz.timezone(timezone)

        # Get current time in the provider's timezone
        fetch_time = datetime.now(tz)

        try:
            # Process each service type
            for service_type in service_types:
                if service_type == "VehiclePosition":
                    result["VehiclePosition"] = cls._process_vehicle_positions(
                        feed, fetch_time
                    )
                elif service_type == "TripUpdate":
                    result["TripUpdate"] = cls._process_trip_updates(feed, fetch_time)
                elif service_type == "Alert":
                    result["Alert"] = cls._process_alerts(feed, fetch_time)
                elif service_type == "TripModifications":
                    result["TripModifications"] = cls._process_trip_modifications(
                        feed, fetch_time
                    )
                else:
                    logger.warning(f"Unknown service type: {service_type}")
        except Exception as e:
            raise GtfsRtParseError(
                f"Error parsing {service_type} data: {str(e)}"
            ) from e

        logger.debug(f"Successfully processed {len(result)} service types from {url}")
        return result

    @staticmethod
    def _process_vehicle_positions(
        feed: gtfs_realtime_pb2.FeedMessage, fetch_time: datetime
    ) -> pd.DataFrame:
        """
        Process vehicle positions from a GTFS-RT feed.

        @param feed: GTFS-RT feed message
        @param fetch_time: Time when the data was fetched
        @return Pandas dataframe with vehicle positions
        @raises GtfsRtParseError: If parsing the vehicle positions fails
        """
        logger = setup_logger(f"{__name__}._process_vehicle_positions")

        # Create lists to store the data
        records = []

        try:
            # Process each entity
            for entity in feed.entity:
                if entity.HasField("vehicle"):
                    # Extract vehicle data
                    vehicle = entity.vehicle
                    record = {
                        "id": entity.id,
                        "fetch_time": fetch_time,
                        "vehicle_id": (
                            vehicle.vehicle.id if vehicle.HasField("vehicle") else None
                        ),
                        "trip_id": (
                            vehicle.trip.trip_id if vehicle.HasField("trip") else None
                        ),
                        "route_id": (
                            vehicle.trip.route_id if vehicle.HasField("trip") else None
                        ),
                        "start_time": (
                            vehicle.trip.start_time
                            if vehicle.HasField("trip")
                            else None
                        ),
                        "start_date": (
                            vehicle.trip.start_date
                            if vehicle.HasField("trip")
                            else None
                        ),
                        "schedule_relationship": (
                            vehicle.trip.schedule_relationship
                            if vehicle.HasField("trip")
                            else None
                        ),
                        "latitude": (
                            vehicle.position.latitude
                            if vehicle.HasField("position")
                            else None
                        ),
                        "longitude": (
                            vehicle.position.longitude
                            if vehicle.HasField("position")
                            else None
                        ),
                        "bearing": (
                            vehicle.position.bearing
                            if vehicle.HasField("position")
                            else None
                        ),
                        "odometer": (
                            vehicle.position.odometer
                            if vehicle.HasField("position")
                            else None
                        ),
                        "speed": (
                            vehicle.position.speed
                            if vehicle.HasField("position")
                            else None
                        ),
                        "stop_id": (
                            vehicle.stop_id if vehicle.HasField("stop_id") else None
                        ),
                        "current_status": (
                            vehicle.current_status
                            if vehicle.HasField("current_status")
                            else None
                        ),
                        "timestamp": (
                            datetime.fromtimestamp(
                                vehicle.timestamp, tz=fetch_time.tzinfo
                            )
                            if vehicle.HasField("timestamp")
                            else None
                        ),
                        "congestion_level": (
                            vehicle.congestion_level
                            if vehicle.HasField("congestion_level")
                            else None
                        ),
                        "occupancy_status": (
                            vehicle.occupancy_status
                            if vehicle.HasField("occupancy_status")
                            else None
                        ),
                    }
                    records.append(record)

            # Create dataframe
            df = pd.DataFrame(records)
            logger.debug(f"Processed {len(df)} vehicle positions")
            return df
        except Exception as e:
            raise GtfsRtParseError(
                f"Error processing vehicle positions: {str(e)}"
            ) from e

    @staticmethod
    def _process_trip_updates(
        feed: gtfs_realtime_pb2.FeedMessage, fetch_time: datetime
    ) -> pd.DataFrame:
        """
        Process trip updates from a GTFS-RT feed.

        @param feed: GTFS-RT feed message
        @param fetch_time: Time when the data was fetched
        @return Pandas dataframe with trip updates
        @raises GtfsRtParseError: If parsing the trip updates fails
        """
        logger = setup_logger(f"{__name__}._process_trip_updates")

        # Create lists to store the data
        records = []

        try:
            # Process each entity
            for entity in feed.entity:
                if entity.HasField("trip_update"):
                    trip_update = entity.trip_update

                    # Extract stop time updates
                    for stop_time_update in trip_update.stop_time_update:
                        record = {
                            "id": entity.id,
                            "fetch_time": fetch_time,
                            "trip_id": (
                                trip_update.trip.trip_id
                                if trip_update.HasField("trip")
                                else None
                            ),
                            "route_id": (
                                trip_update.trip.route_id
                                if trip_update.HasField("trip")
                                else None
                            ),
                            "start_time": (
                                trip_update.trip.start_time
                                if trip_update.HasField("trip")
                                else None
                            ),
                            "start_date": (
                                trip_update.trip.start_date
                                if trip_update.HasField("trip")
                                else None
                            ),
                            "schedule_relationship": (
                                trip_update.trip.schedule_relationship
                                if trip_update.HasField("trip")
                                else None
                            ),
                            "vehicle_id": (
                                trip_update.vehicle.id
                                if trip_update.HasField("vehicle")
                                else None
                            ),
                            "timestamp": (
                                datetime.fromtimestamp(
                                    trip_update.timestamp, tz=fetch_time.tzinfo
                                )
                                if trip_update.HasField("timestamp")
                                else None
                            ),
                            "stop_sequence": (
                                stop_time_update.stop_sequence
                                if stop_time_update.HasField("stop_sequence")
                                else None
                            ),
                            "stop_id": (
                                stop_time_update.stop_id
                                if stop_time_update.HasField("stop_id")
                                else None
                            ),
                            "arrival_delay": (
                                stop_time_update.arrival.delay
                                if stop_time_update.HasField("arrival")
                                else None
                            ),
                            "arrival_time": (
                                datetime.fromtimestamp(
                                    stop_time_update.arrival.time, tz=fetch_time.tzinfo
                                )
                                if stop_time_update.HasField("arrival")
                                and stop_time_update.arrival.HasField("time")
                                else None
                            ),
                            "arrival_uncertainty": (
                                stop_time_update.arrival.uncertainty
                                if stop_time_update.HasField("arrival")
                                and stop_time_update.arrival.HasField("uncertainty")
                                else None
                            ),
                            "departure_delay": (
                                stop_time_update.departure.delay
                                if stop_time_update.HasField("departure")
                                else None
                            ),
                            "departure_time": (
                                datetime.fromtimestamp(
                                    stop_time_update.departure.time,
                                    tz=fetch_time.tzinfo,
                                )
                                if stop_time_update.HasField("departure")
                                and stop_time_update.departure.HasField("time")
                                else None
                            ),
                            "departure_uncertainty": (
                                stop_time_update.departure.uncertainty
                                if stop_time_update.HasField("departure")
                                and stop_time_update.departure.HasField("uncertainty")
                                else None
                            ),
                            "schedule_relationship_update": (
                                stop_time_update.schedule_relationship
                                if stop_time_update.HasField("schedule_relationship")
                                else None
                            ),
                        }
                        records.append(record)

            # Create dataframe
            df = pd.DataFrame(records)
            logger.debug(f"Processed {len(df)} trip updates")
            return df
        except Exception as e:
            raise GtfsRtParseError(f"Error processing trip updates: {str(e)}") from e

    @staticmethod
    def _process_alerts(
        feed: gtfs_realtime_pb2.FeedMessage, fetch_time: datetime
    ) -> pd.DataFrame:
        """
        Process alerts from a GTFS-RT feed.

        @param feed: GTFS-RT feed message
        @param fetch_time: Time when the data was fetched
        @return Pandas dataframe with alerts
        @raises GtfsRtParseError: If parsing the alerts fails
        """
        logger = setup_logger(f"{__name__}._process_alerts")

        # Create lists to store the data
        records = []

        try:
            # Process each entity
            for entity in feed.entity:
                if entity.HasField("alert"):
                    alert = entity.alert

                    # Extract all informed entities
                    for informed_entity in alert.informed_entity:
                        record = {
                            "id": entity.id,
                            "fetch_time": fetch_time,
                            "agency_id": (
                                informed_entity.agency_id
                                if informed_entity.HasField("agency_id")
                                else None
                            ),
                            "route_id": (
                                informed_entity.route_id
                                if informed_entity.HasField("route_id")
                                else None
                            ),
                            "route_type": (
                                informed_entity.route_type
                                if informed_entity.HasField("route_type")
                                else None
                            ),
                            "stop_id": (
                                informed_entity.stop_id
                                if informed_entity.HasField("stop_id")
                                else None
                            ),
                            "trip_id": (
                                informed_entity.trip.trip_id
                                if informed_entity.HasField("trip")
                                else None
                            ),
                            "trip_route_id": (
                                informed_entity.trip.route_id
                                if informed_entity.HasField("trip")
                                else None
                            ),
                            "trip_start_time": (
                                informed_entity.trip.start_time
                                if informed_entity.HasField("trip")
                                else None
                            ),
                            "trip_start_date": (
                                informed_entity.trip.start_date
                                if informed_entity.HasField("trip")
                                else None
                            ),
                            "cause": alert.cause if alert.HasField("cause") else None,
                            "effect": (
                                alert.effect if alert.HasField("effect") else None
                            ),
                            "url": (
                                alert.url.translation[0].text
                                if alert.HasField("url")
                                and len(alert.url.translation) > 0
                                else None
                            ),
                            "header_text": (
                                alert.header_text.translation[0].text
                                if alert.HasField("header_text")
                                and len(alert.header_text.translation) > 0
                                else None
                            ),
                            "description_text": (
                                alert.description_text.translation[0].text
                                if alert.HasField("description_text")
                                and len(alert.description_text.translation) > 0
                                else None
                            ),
                            "start_time": (
                                datetime.fromtimestamp(
                                    alert.active_period[0].start, tz=fetch_time.tzinfo
                                )
                                if len(alert.active_period) > 0
                                and alert.active_period[0].HasField("start")
                                else None
                            ),
                            "end_time": (
                                datetime.fromtimestamp(
                                    alert.active_period[0].end, tz=fetch_time.tzinfo
                                )
                                if len(alert.active_period) > 0
                                and alert.active_period[0].HasField("end")
                                else None
                            ),
                        }
                        records.append(record)

            # Create dataframe
            df = pd.DataFrame(records)
            logger.debug(f"Processed {len(df)} alerts")
            return df
        except Exception as e:
            raise GtfsRtParseError(f"Error processing alerts: {str(e)}") from e

    @staticmethod
    def _process_trip_modifications(
        feed: gtfs_realtime_pb2.FeedMessage, fetch_time: datetime
    ) -> pd.DataFrame:
        """
        Process trip modifications from a GTFS-RT feed.

        @param feed: GTFS-RT feed message
        @param fetch_time: Time when the data was fetched
        @return Pandas dataframe with trip modifications
        @raises GtfsRtParseError: If parsing the trip modifications fails
        """
        logger = setup_logger(f"{__name__}._process_trip_modifications")
        logger.warning("Trip modifications processing not implemented yet")

        # Create an empty dataframe
        df = pd.DataFrame()

        return df
