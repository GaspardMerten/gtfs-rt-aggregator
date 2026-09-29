import hashlib
from datetime import datetime, timezone as dt_timezone
from typing import Dict, List, Any, Optional

import pyarrow as pa
import requests
from google.transit import gtfs_realtime_pb2

from ..schema.alert import alert_schema
from ..schema.shape import shape_schema
from ..schema.stop import stop_schema
from ..schema.trip_modifications import trip_modifications_schema
from ..schema.trip_update import trip_update_schema
from ..schema.vehicle_position import vehicle_position_schema
from ..fetcher.arrow_builder import TableBuilder
from ..schema.conform import flat
from ..utils import setup_logger
from ..utils.http import get_bytes

SERVICE_TYPE_TO_SCHEMA = {
    "VehiclePosition": vehicle_position_schema,
    "TripUpdate": trip_update_schema,
    "Alert": alert_schema,
    "TripModifications": trip_modifications_schema,
    "Shape": shape_schema,
    "Stop": stop_schema,
}
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
        @return Tables by service type (no rows for service types absent
            from the feed: the fetch is still recorded, see fetch_times.py)
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
        for entity, content_hash in zip(entities, hashes):
            for field, (service_type, builder) in builders.items():
                if entity.HasField(field):
                    builder.add(entity.id, content_hash, getattr(entity, field))
        result = {}
        for service_type, builder in builders.values():
            table = result[service_type] = flat(builder.finish(constants))
            cls.logger.info(
                f"Processed {table.num_rows} records for service type {service_type}"
            )
        return result
