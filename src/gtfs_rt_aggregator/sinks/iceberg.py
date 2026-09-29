"""
Iceberg tables over the compacted days (optional extra: iceberg).

One table per service type (e.g. archive.TripUpdate), stored in the global
storage under <warehouse>/<service>/. Compacted day files are registered as
they are (add_files: no copy, no rewrite), partitioned by provider and local
date. A day compacted again (late files) replaces its earlier file in the same
commit. version-hint.text lets readers without a catalog find the latest
metadata, e.g. DuckDB: iceberg_scan('s3://bucket/iceberg/TripUpdate').
"""

import logging
import posixpath
import warnings
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytz

from ..aggregator.convert import aggregated_root
from ..config.models import GtfsRtConfig, StorageConfig
from ..storage.base import StorageInterface

logger = logging.getLogger(__name__)


def _io_properties(storage: StorageConfig) -> Dict[str, str]:
    """PyIceberg FileIO settings matching the storage."""
    params = storage.params
    if storage.type in ("minio", "s3"):
        scheme = "https" if params.get("secure", True) else "http"
        return {
            "s3.endpoint": f"{scheme}://{params['endpoint']}",
            "s3.access-key-id": params["access_key"],
            "s3.secret-access-key": params["secret_key"],
            "s3.region": params.get("region", "us-east-1"),
        }
    return {}


class IcebergSink:
    def __init__(self, config: GtfsRtConfig, storages: Dict[str, StorageInterface]):
        if config.iceberg is None:
            raise ValueError("No [iceberg] section in the configuration")
        self.config = config
        self.settings = config.iceberg
        self.storage = storages["global"]
        self.storages = storages
        self._catalog = None

    # Catalog and tables ----------------------------------------------------------

    @property
    def catalog(self):
        if self._catalog is None:
            warehouse = self.storage.uri(self.settings.warehouse)
            properties = {"warehouse": warehouse, **_io_properties(self.config.storage)}
            if self.settings.catalog == "sql":
                from pyiceberg.catalog.sql import SqlCatalog

                self._catalog = SqlCatalog(
                    "gtfs_rt_aggregator", uri=self.settings.catalog_uri, **properties
                )
            else:
                from pyiceberg.catalog import load_catalog

                self._catalog = load_catalog(
                    "gtfs_rt_aggregator",
                    type="rest",
                    uri=self.settings.catalog_uri,
                    **properties,
                )
            self._catalog.create_namespace_if_not_exists(self.settings.namespace)
        return self._catalog

    def _table(self, service: str, schema):
        """The service's table, created from the first file registered."""
        from pyiceberg.exceptions import NoSuchTableError

        identifier = f"{self.settings.namespace}.{service}"
        try:
            return self.catalog.load_table(identifier)
        except NoSuchTableError:
            table = self.catalog.create_table(
                identifier,
                schema=schema,
                location=self.storage.uri(f"{self.settings.warehouse}/{service}"),
            )
            with table.update_spec() as spec:
                spec.add_identity("provider")
                spec.add_identity("date")
            logger.info(f"Created Iceberg table {identifier}")
            return table

    # Registration ------------------------------------------------------------------

    def sync(self, days_back: Optional[int] = 7) -> int:
        """
        Register the compacted days not registered yet (or compacted again
        since). Looks at the last days_back finished days, or every day stored
        when days_back is None. Returns the number of days registered.
        """
        registered = 0
        name = self.config.output.compacted_name
        for service in self.settings.services:
            table = None
            known: Optional[Dict[str, int]] = None
            for provider in self.config.providers:
                if not any(service in api.services for api in provider.realtime):
                    continue
                storage = self.storages.get(provider.name, self.storage)
                if storage is not self.storage:
                    logger.warning(
                        f"Iceberg: {provider.name} has its own storage, not registered "
                        "(tables and data must share the global storage)"
                    )
                    continue
                for path in self._day_files(provider, service, name, days_back):
                    uri = storage.uri(path)
                    size = storage.file_size(path)
                    if table is None:
                        table = self._existing_table(service)
                    if table is not None and known is None:
                        files = (
                            table.inspect.files()
                            .select(["file_path", "file_size_in_bytes"])
                            .to_pydict()
                        )
                        known = dict(
                            zip(files["file_path"], files["file_size_in_bytes"])
                        )
                    if known is not None and known.get(uri) == size:
                        continue
                    table, done = self._register(
                        service, provider.name, path, uri, table
                    )
                    if done:
                        known = None  # read again after a commit
                        registered += 1
            if table is not None and self.settings.write_version_hint:
                self._write_version_hint(service, table)
        return registered

    def _existing_table(self, service: str):
        from pyiceberg.exceptions import NoSuchTableError

        try:
            return self.catalog.load_table(f"{self.settings.namespace}.{service}")
        except NoSuchTableError:
            return None

    def _day_files(self, provider, service, name, days_back) -> Iterable[str]:
        storage = self.storages.get(provider.name, self.storage)
        if days_back is None:
            root = aggregated_root(self.config, provider.name, service)
            return sorted(
                p for p in storage.walk_files(root) if posixpath.basename(p) == name
            )
        tz = pytz.timezone(provider.timezone)
        today = datetime.now(tz).date()
        paths = []
        for back in range(1, days_back + 1):
            day = today - timedelta(days=back)
            start = tz.localize(datetime(day.year, day.month, day.day))
            folder = posixpath.dirname(
                self.config.output.path_template.format(
                    provider=provider.name, service=service, start=start, end=start
                )
            )
            path = posixpath.join(folder, name)
            if storage.file_exists(path):
                paths.append(path)
        return paths

    def _register(self, service: str, provider: str, path: str, uri: str, table):
        """
        Add a day file, replacing the day's earlier file, in one commit.
        Returns the table and whether the file was registered.
        """
        import tempfile

        from pyiceberg.expressions import And, EqualTo

        with tempfile.NamedTemporaryFile(suffix=".parquet") as local:
            self.storage.read_to_file(path, local.name)
            schema = pq.read_schema(local.name)
            if (
                pa.types.is_integer(schema.field("fetchTime").type)
                or "date" not in schema.names
            ):
                logger.warning(
                    f"Iceberg: {path} was written before 0.6.0, not registered: "
                    "run --iceberg-backfill"
                )
                return table, False
            dates = pq.read_table(local.name, columns=["date"])["date"].unique()
        if len(dates) != 1:
            raise ValueError(f"{path} holds {len(dates)} dates, expected one")
        day = dates[0].as_py().isoformat()

        if table is None:
            table = self._table(service, schema.remove_metadata())
        with table.transaction() as transaction:
            # A new column (newer version of this package): add it to the table
            with transaction.update_schema() as update:
                update.union_by_name(schema.remove_metadata())
            with warnings.catch_warnings():
                # "did not match any records": the day's first registration
                warnings.simplefilter("ignore")
                transaction.delete(
                    And(EqualTo("provider", provider), EqualTo("date", day))
                )
            transaction.add_files([uri])
        logger.info(f"Iceberg: registered {service} {provider} {day}")
        return table, True

    def _write_version_hint(self, service: str, table):
        """metadata/version-hint.text: name of the latest metadata file, without .metadata.json."""
        table.refresh()
        name = posixpath.basename(table.metadata_location).removesuffix(
            ".metadata.json"
        )
        self.storage.save_bytes(
            name.encode(),
            f"{self.settings.warehouse}/{service}/metadata/version-hint.text",
        )

    # Maintenance -------------------------------------------------------------------

    def maintain(self):
        """Expire snapshots older than expire_snapshots_days."""
        older_than = datetime.now(timezone.utc) - timedelta(
            days=self.settings.expire_snapshots_days
        )
        for service in self.settings.services:
            table = self._existing_table(service)
            if table is None:
                continue
            table.maintenance.expire_snapshots().older_than(older_than).commit()
            if self.settings.write_version_hint:
                self._write_version_hint(service, table)
            logger.info(
                f"Iceberg: expired snapshots of {service} older than {older_than}"
            )
