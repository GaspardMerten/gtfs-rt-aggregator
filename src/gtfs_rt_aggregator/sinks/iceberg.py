"""
Iceberg tables over the compacted days (optional extra: iceberg).

One table per service type (e.g. archive.TripUpdate), stored in the global
storage under <warehouse>/<service>/. Compacted day files are registered as
they are (add_files: no copy, no rewrite), partitioned by provider and local
date. A day compacted again (late files) replaces its earlier file in the same
commit. version-hint.text lets readers without a catalog find the latest
metadata, e.g. DuckDB: iceberg_scan('s3://bucket/iceberg/TripUpdate').

With public_base_url, a copy of each table's current metadata (metadata.json,
manifest lists, manifests) is kept in public_warehouse with every path
rewritten to <public_base_url>/<object key>, so readers only ever see that
host: iceberg_scan('https://data.example.org/iceberg-public/TripUpdate'). The
catalog and PyIceberg keep using the storage paths.
"""

import io
import json
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

# Commits tried when another process commits to the same table meanwhile
COMMIT_ATTEMPTS = 3


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
        from pyiceberg.exceptions import NoSuchTableError, TableAlreadyExistsError

        identifier = f"{self.settings.namespace}.{service}"
        try:
            return self.catalog.load_table(identifier)
        except NoSuchTableError:
            try:
                table = self.catalog.create_table(
                    identifier,
                    schema=schema,
                    location=self.storage.uri(f"{self.settings.warehouse}/{service}"),
                )
            except TableAlreadyExistsError:
                # Created by another process meanwhile
                return self.catalog.load_table(identifier)
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
                if not self._stores(provider, service):
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
            if self.settings.public_base_url:
                table = table or self._existing_table(service)
                if table is not None:
                    self._publish(service, table)
        return registered

    def _stores(self, provider, service: str) -> bool:
        if service == "TripStopEvent":
            return self.config.output.trip_stop_events and any(
                "TripUpdate" in api.services for api in provider.realtime
            )
        return any(service in api.services for api in provider.realtime)

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
            if "date" not in schema.names or (
                "fetchTime" in schema.names
                and pa.types.is_integer(schema.field("fetchTime").type)
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

        from pyiceberg import exceptions

        # ValidationException: "Added data files were found matching the
        # filter" (PyIceberg 0.10+ checks concurrent commits itself)
        conflicts = tuple(
            getattr(exceptions, name)
            for name in ("CommitFailedException", "ValidationException")
            if hasattr(exceptions, name)
        )

        if table is None:
            table = self._table(service, schema.remove_metadata())
        size = self.storage.file_size(path)
        for attempt in range(COMMIT_ATTEMPTS):
            try:
                with table.transaction() as transaction:
                    # A new column (newer version of this package): add it
                    with transaction.update_schema() as update:
                        update.union_by_name(schema.remove_metadata())
                    with warnings.catch_warnings():
                        # "did not match any records": the day's first registration
                        warnings.simplefilter("ignore")
                        transaction.delete(
                            And(EqualTo("provider", provider), EqualTo("date", day))
                        )
                    transaction.add_files([uri])
                break
            except conflicts:
                # Another process committed first (e.g. --iceberg-backfill next
                # to the pipeline): done if it registered this very file
                table.refresh()
                files = (
                    table.inspect.files()
                    .select(["file_path", "file_size_in_bytes"])
                    .to_pydict()
                )
                if (
                    dict(zip(files["file_path"], files["file_size_in_bytes"])).get(uri)
                    == size
                ):
                    return table, False
                if attempt == COMMIT_ATTEMPTS - 1:
                    raise
                logger.info(f"Iceberg: concurrent commit on {service}, retrying")
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
            if self.settings.public_base_url:
                self._publish(service, table)
            logger.info(
                f"Iceberg: expired snapshots of {service} older than {older_than}"
            )

    # Public copy -------------------------------------------------------------------

    def publish(self, force: bool = False) -> int:
        """Write the public copy of every table's metadata. Returns the tables published."""
        if not self.settings.public_base_url:
            raise ValueError("Set iceberg.public_base_url to publish the tables")
        published = 0
        for service in self.settings.services:
            table = self._existing_table(service)
            if table is not None:
                published += self._publish(service, table, force)
        return published

    def _publish(self, service: str, table, force: bool = False) -> bool:
        """
        Copy the table's current metadata to the public folder with public
        paths. Manifests and manifest lists never change once written: only
        the new ones are copied. Public metadata files no longer used are
        deleted. Returns whether a new version was published.
        """
        import fastavro

        table.refresh()
        storage = self.storage
        root = storage.uri("x")[:-1]  # storage URI of the empty path
        location = table.metadata.location.rstrip("/")
        public_folder = f"{self.settings.public_warehouse}/{service}"
        public_location = f"{self.settings.public_base_url}/{public_folder}"

        def key(uri: str) -> str:
            if not uri.startswith(root):
                raise ValueError(f"{uri} is not in the storage ({root})")
            return uri[len(root) :]

        def public_key(uri: str) -> str:
            """Where the public copy of a metadata file goes."""
            return public_folder + uri[len(location) :]

        def rewrite(value):
            if isinstance(value, str):
                if value == location or value.startswith(location + "/"):
                    return public_location + value[len(location) :]
                if value.startswith(root):
                    return f"{self.settings.public_base_url}/{value[len(root):]}"
                return value
            if isinstance(value, dict):
                return {k: rewrite(v) for k, v in value.items()}
            if isinstance(value, list):
                return [rewrite(v) for v in value]
            return value

        def copy_avro(uri: str, change=None) -> int:
            """Rewrite an Avro metadata file to its public copy; returns its size."""
            reader = fastavro.reader(io.BytesIO(storage.read_bytes(key(uri))))
            schema = json.loads(reader.metadata["avro.schema"])
            records = [rewrite(record) for record in reader]
            if change:
                records = [change(record) for record in records]
            out = io.BytesIO()
            fastavro.writer(
                out,
                schema,
                records,
                codec=reader.codec,
                metadata={
                    k: v
                    for k, v in reader.metadata.items()
                    if not k.startswith("avro.")
                },
            )
            storage.save_bytes(out.getvalue(), public_key(uri))
            return len(out.getvalue())

        metadata_uri = table.metadata_location
        target = public_key(metadata_uri)
        if storage.file_exists(target) and not force:
            return False
        metadata = json.loads(storage.read_bytes(key(metadata_uri)))
        kept = {target}
        for snapshot in metadata.get("snapshots", []):
            manifest_list = snapshot["manifest-list"]
            kept.add(public_key(manifest_list))
            if storage.file_exists(public_key(manifest_list)) and not force:
                reader = fastavro.reader(
                    io.BytesIO(storage.read_bytes(public_key(manifest_list)))
                )
                kept.update(
                    public_key(location + r["manifest_path"][len(public_location) :])
                    for r in reader
                )
                continue
            sizes = {}
            reader = fastavro.reader(io.BytesIO(storage.read_bytes(key(manifest_list))))
            for record in reader:
                manifest = record["manifest_path"]
                kept.add(public_key(manifest))
                if storage.file_exists(public_key(manifest)) and not force:
                    sizes[manifest] = storage.file_size(public_key(manifest))
                else:
                    sizes[manifest] = copy_avro(manifest)
            lengths = {rewrite(path): size for path, size in sizes.items()}
            copy_avro(
                manifest_list,
                lambda r: {**r, "manifest_length": lengths[r["manifest_path"]]},
            )
        public = rewrite(metadata)
        # Earlier metadata files are not copied
        public["metadata-log"] = []
        storage.save_bytes(json.dumps(public).encode(), target)
        name = posixpath.basename(target).removesuffix(".metadata.json")
        storage.save_bytes(name.encode(), f"{public_folder}/metadata/version-hint.text")
        kept.add(f"{public_folder}/metadata/version-hint.text")

        folder = f"{public_folder}/metadata"
        for path in storage.list_files(folder):
            if posixpath.dirname(path) == folder and path not in kept:
                storage.delete_file(path)
        logger.info(f"Iceberg: published {service} to {public_location}")
        return True
