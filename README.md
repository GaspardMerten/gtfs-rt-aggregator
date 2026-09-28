# GTFS-RT Aggregator

This project provides a pipeline for fetching, storing, and aggregating GTFS-RT (General Transit Feed Specification -
Realtime) data from multiple providers into Parquet format. It can also keep a Parquet copy of each version of the
providers' GTFS static feeds.

## Features

- Fetch GTFS-RT data from multiple providers and APIs, with retries, skipping fetches where nothing changed
- Keep only some rows, e.g. rail from a mixed-mode feed (route types resolved through the static feed)
- Store a new copy of each GTFS static feed when it changes, including feeds whose URL changes (optional)
- Store Parquet files with multiple storage backends (filesystem, Google Cloud Storage, MinIO), in Hive-style folders
- Aggregate data files based on configurable time intervals, optionally merging identical rows (`firstSeen` / `lastSeen`)
  and compacting each day into one sorted file
- A status file per feed: last success, last error, feed age, entity count
- Configurable via a single TOML configuration file, with secrets from environment variables

## Requirements

- Python 3.11+
- Installed with the package: requests, gtfs-realtime-bindings, protobuf, pandas, pyarrow, pytz, schedule, pydantic,
  google-cloud-storage and minio
- Only with the `static` extra: gtfs-parquet (which brings in Polars), for GTFS static feeds

## Installation

### From PyPI (recommended)

```bash
pip install gtfs-rt-aggregator
```

To also store GTFS static feeds:

```bash
pip install "gtfs-rt-aggregator[static]"
```

### From Source

1. Clone this repository:
   ```bash
   git clone https://github.com/GaspardMerten/gtfs-rt-aggregator
   cd gtfs-rt-aggregator
   ```

2. Install in development mode:
   ```bash
   pip install -e .
   ```

## Configuration

The pipeline is configured using a TOML configuration file. Here's an example:

```toml
[storage]
type = "filesystem"  # Options: "filesystem", "gcs", or "minio"
[storage.params]
base_directory = "data"  # Base directory for filesystem storage

[[providers]]
name = "ovapi"
timezone = "Europe/Amsterdam"

  [[providers.realtime]]
  url = "https://gtfs.ovapi.nl/nl/vehiclePositions.pb"
  services = ["VehiclePosition"]
  refresh_seconds = 20  # Fetch every 20 seconds
  frequency_minutes = 60  # Group files in 60-minute intervals
  check_interval_seconds = 300  # Check for new files every 5 minutes
  deduplicate = true  # Merge identical consecutive rows of a vehicle

  [[providers.realtime]]
  url = "https://gtfs.ovapi.nl/nl/tripUpdates.pb"
  services = ["TripUpdate"]
  refresh_seconds = 20
  [providers.realtime.filter]
  route_types = [2, "100-199"]  # Rail only, through the static feed below

  [[providers.static]]
  url = "https://gtfs.ovapi.nl/nl/gtfs-nl.zip"
  check_minutes = 60  # Check for a new version every hour

[[providers]]
name = "lu"
timezone = "Europe/Luxembourg"

  # Luxembourg publishes each new version under a new URL: find it on the dataset's page
  [[providers.static]]
  index_url = "https://data.public.lu/api/1/datasets/horaires-et-arrets-des-transport-publics-gtfs/"
  url_pattern = 'https://download\.data\.public\.lu/resources/horaires-et-arrets-des-transport-publics-gtfs/\d{8}-\d{6}/gtfs-[\d-]+\.zip'
  check_minutes = 1440

[[providers]]
name = "uk"
timezone = "Europe/London"

  [[providers.realtime]]
  url = "https://example.org/uk/gtfsrt.pb"
  services = ["VehiclePosition"]
  [providers.realtime.headers]
  x-api-key = "${UK_API_KEY}"  # Read from the UK_API_KEY environment variable
```

### Storage Backend Examples

#### Google Cloud Storage

```toml
[storage]
type = "gcs"
[storage.params]
bucket_name = "my-gtfs-bucket"
base_path = "gtfs-data"  # Optional: subfolder within the bucket
# Authentication is handled via the GOOGLE_APPLICATION_CREDENTIALS environment variable
```

#### MinIO Storage

```toml
[storage]
type = "minio"
[storage.params]
endpoint = "minio.example.com:9000"
access_key = "YOUR_ACCESS_KEY"
secret_key = "YOUR_SECRET_KEY"
bucket_name = "gtfs-data"
secure = true  # Use HTTPS
base_path = "gtfs-feeds"  # Optional: subfolder within the bucket
```

### Configuration Options

- **storage**: Global storage configuration
  - **type**: Storage backend type ("filesystem", "gcs", or "minio")
  - **params**: Backend-specific parameters (`bucket_name` is required for GCS; `endpoint`, `access_key`, `secret_key`
    and `bucket_name` for MinIO)

- **output**: Optional, where aggregated files go
  - **path_template**: Path of each aggregated file. Fields: `provider`, `service`, `start` and `end` (the period, in
    the provider's timezone, with `strftime` formats). Default:
    `"provider={provider}/service={service}/date={start:%Y-%m-%d}/{start:%H-%M-%S}_to_{end:%H-%M-%S}.parquet"`
  - **compact_daily**: Once a day is over, merge its aggregated files into one file (default `false`). Needs one
    folder per day in `path_template`. The whole day is read in memory.
  - **compacted_name**: Name of that file, in the day's folder (default `"day.parquet"`)
  - **sort_by**: Columns the compacted file is sorted by (default `["entityId", "fetchTime"]`)

- **providers**: List of data providers
  - **name**: Name of the provider (used for directory structure, must be unique)
  - **timezone**: Timezone for the provider's data (default `"UTC"`)
  - **storage**: Optional storage for this provider only (same fields as the global one)
  - **realtime**: List of GTFS-RT feeds for this provider (called `apis` before 0.3.0, which still works)
    - **url**: URL of the GTFS-RT feed
    - **headers**: Optional HTTP headers sent with each request, e.g. an API key
    - **retries**: Retries on connection errors, TLS errors, timeouts and HTTP 429/5xx, waiting 1 s, 2 s, 4 s… (default `3`)
    - **skip_unchanged**: Do not store a fetch whose entities are all the same as the previous fetch's (default `true`)
    - **deduplicate**: When aggregating, merge consecutive identical rows of an entity (default `false`, see below)
    - **filter**: Rows to keep (default: all), see below
    - **static**: Name of the provider's static feed to use for `staticVersion` and the filter, if it has several
    - **services**: List of service types to extract from the feed (VehiclePosition, TripUpdate, Alert, TripModifications)
    - **refresh_seconds**: How often to fetch data from this API (default `60`)
    - **frequency_minutes**: The time interval (in minutes) for grouping files (default `60`)
    - **check_interval_seconds**: How often to check for new files to aggregate (default `300`)
    - **accumulate_minutes**: Keep fetches in memory and write them in blocks of this many minutes (default `0`: write every fetch right away). Blocks follow the clock in the provider's timezone: `15` gives 16:00-16:15, 16:15-16:30, and so on, and `1440` gives one block per day. The value must divide 1440 and `frequency_minutes`. A block is written when the next one starts, or when the pipeline stops cleanly. If the process is killed, the current block is lost. A whole block sits in memory, so use short blocks for large feeds.
    - **accumulate_concatenate**: Write each block as one Parquet file instead of one file per fetch (default `true`)
  - **static**: List of GTFS static feeds for this provider (needs the `static` extra)
    - **url**: URL of the GTFS zip
    - **index_url** and **url_pattern**: Instead of `url`, for feeds published under a new URL for each version: the
      page (HTML or JSON) at `index_url` is read, and the greatest link matching the regular expression `url_pattern` is
      downloaded. With dated URLs, that is the newest.
    - **check_minutes**: How often to check for a new version (default `60`)
    - **name**: Folder the versions are stored in (default `static`). Only needed when a provider has several static feeds.
    - **headers**: Optional HTTP headers sent with each request
    - **retries**: As for realtime feeds (default `3`)
    - **reuse_unchanged_tables**: Point to the previous version's file for tables whose source file did not change,
      instead of storing them again (default `false`: each version is a full copy)

A provider can have realtime feeds, static feeds, or both. Every job runs once at startup, then at its interval.

Any string in the file can read an environment variable with `${NAME}`, e.g. `x-api-key = "${UK_API_KEY}"` or
`url = "https://example.org/feed?key=${KEY}"`, so keys can stay out of the file. Loading fails if the variable is not
set. Write `$${` for a literal `${`.

### Realtime rows

Besides the entity's fields, each row has:

- `fetchTime`: when it was fetched (Unix time)
- `feedTimestamp`: the time in the feed's header (Unix time)
- `staticVersion`: the provider's static version current at that time, to join with the right timetable later
- `contentHash`: a hash of the entity, used to skip unchanged fetches and to deduplicate
- with `deduplicate = true`, after aggregation: `firstSeen` and `lastSeen`

**Unchanged fetches.** With `skip_unchanged` (the default), a fetch is not stored when its entities are exactly those of
the previous fetch, in any order, even if the header time changed. Polling every 30 s a feed that updates every 2 min
then stores one copy instead of four.

**Deduplication.** With `deduplicate = true`, consecutive rows of the same entity with the same content become one row,
with `firstSeen` and `lastSeen` its first and last `fetchTime`. A vehicle that stands still for ten minutes is one row,
not twenty. If the entity changes and then comes back to an earlier state, that is a new row. Fetches skipped as
unchanged are not counted, so `lastSeen` is the last *stored* fetch.

**Filter.** `[providers.realtime.filter]` keeps the rows matching any of:

- `route_types`: GTFS route types, as numbers or ranges, e.g. `[2, "100-199"]` for rail
- `route_ids`: route ids
- `trip_ids`: trip ids

Realtime entities often carry only a trip id, so `route_types` and `route_ids` are resolved through the trips and
routes of the provider's latest static version: the provider needs a `[[providers.static]]` feed. Trip updates and
vehicle positions are matched by trip or route (a vehicle without a trip is dropped), alerts by any informed route,
route type or trip; other entity types are kept. Until the first static version is stored, rows are not filtered.

**Status.** After each fetch, `<provider>/_status/<services>-<id>.json` holds the last attempt, last success, last
error, the feed's age (fetch time minus header time), the number of entities before and after the filter, and whether
the fetch was unchanged. The URL is stored without its query string, which may hold a key.

### Storage Layout

```
ovapi/VehiclePosition/individual/2026-09-28_14-00-20Z.parquet                        # one fetch (or block), UTC
provider=ovapi/service=VehiclePosition/date=2026-09-28/16-00-00_to_17-00-00.parquet  # aggregated, local time
provider=ovapi/service=VehiclePosition/date=2026-09-27/day.parquet                   # compacted day
ovapi/static/2026-09-28_01-00-00Z/stops.parquet                 # one static version (UTC), one file per table
ovapi/static/2026-09-28_01-00-00Z/manifest.json
ovapi/static/latest.json                                        # manifest of the latest version
ovapi/_status/VehiclePosition-1a2b3c4d.json                     # status of a realtime feed
```

Aggregated files are in Hive-style folders (`key=value`), so DuckDB, Polars or BigQuery can read them as one table and
skip folders when filtering on provider, service or date:

```sql
SELECT * FROM read_parquet('data/provider=*/service=VehiclePosition/date=*/*.parquet', hive_partitioning = true)
WHERE date = '2026-09-28'
```

Individual files and static versions are named after their fetch time in UTC (the `Z` suffix), so the hour that repeats when clocks go back never produces the same name twice. Aggregation periods, daily folders and aggregated files use the provider's timezone; on the night clocks go back, the repeated hour ends up in a single aggregated file covering both passes.

A new static version is stored only when a file inside the zip changed. The pipeline first asks the server whether the feed changed since the last check (ETag / Last-Modified), then compares the checksum and size of each file in the zip. A zip rebuilt with the same files is not stored again. Each version is a full copy of the feed (unless `reuse_unchanged_tables` is set: `manifest.json` then gives the path of each table), converted table by table with gtfs-parquet's `convert_gtfs_zip`, so the feed is never fully in memory: the German national feed (a 298 MB zip, 40 million stop times) takes about a minute and 0.5 GB of RAM. Static jobs limit Polars to 4 threads, since its memory grows with the thread count; set `POLARS_MAX_THREADS` to change it. Rows keep the order of the source files. The zip and the converted tables are kept in a temporary folder until uploaded (about 1.5 GB for the German feed): if `/tmp` is in RAM (tmpfs, common in containers), point `TMPDIR` to a disk. If a check is still running when the next one is due, the next one is skipped.

### Upgrading to 0.5.0

- Aggregated files now go to Hive-style folders (`provider=…/service=…/date=…/`). Set `path_template` to keep the old
  layout: `"{provider}/{service}/{start:%Y-%m-%d}/{start:%H-%M-%S}_to_{end:%H-%M-%S}.parquet"`. `filename_format` and
  `time_format` still work but are deprecated.
- Fetches whose entities did not change are no longer stored (`skip_unchanged = false` to keep every fetch).
- New columns: `feedTimestamp`, `staticVersion` and `contentHash`.
- `manifest.json` gives each table's path in `tables` (it was a list of names).
- Unknown options in `[[providers.realtime]]` and `[output]` are now rejected, like typos in `[[providers.static]]`.

### Upgrading to 0.4.0

- Static feeds need gtfs-parquet 0.5.1 or later, and use much less memory (see above).
- Individual files and static versions are now named in UTC (`2026-09-28_14-00-20Z.parquet`) instead of local time with an offset, which had a `+` that some tools read as a space. Files named the old ways are still aggregated.
- The configuration is now validated: invalid service types, timezones, non-positive intervals, duplicate provider names and missing GCS/MinIO parameters are rejected when the file is loaded.

### Upgrading to 0.3.0

- Rename `[[providers.apis]]` to `[[providers.realtime]]`. The old name still works but logs a warning.
- When files arrive for a period that was already aggregated, they are added to the existing aggregated file instead of replacing it.
- `ProviderConfig.model_dump()` returns the feeds under `realtime` instead of `apis`.
- Every job now runs once at startup instead of waiting for its first interval.

## Usage

### Command Line

Run the pipeline with a configuration file:

```bash
gtfs-rt-pipeline configuration.toml
```

You can adjust the logging level with the `--log-level` parameter:

```bash
gtfs-rt-pipeline configuration.toml --log-level DEBUG
```

### Programmatic Usage

```python
from gtfs_rt_aggregator import run_pipeline_from_toml

# Run pipeline from a TOML file
run_pipeline_from_toml("configuration.toml")
```

Or with a configuration object:

```python
from gtfs_rt_aggregator.config.loader import load_config_from_toml
from gtfs_rt_aggregator import run_pipeline

# Load configuration
config = load_config_from_toml("configuration.toml")

# Run pipeline
run_pipeline(config)
```

## Project Structure

```
src/gtfs_rt_aggregator/
  ├── __init__.py                # Package initialization
  ├── pipeline.py                # Main pipeline implementation
  ├── aggregator/                # Aggregation functionality
  ├── config/                    # Configuration loading and validation
  ├── fetcher/                   # GTFS-RT data fetching functionality
  ├── static/                    # GTFS static feed versions
  ├── storage/                   # Storage backend implementations
  └── utils/                     # Utility functions and helpers
      ├── cli.py                 # Command-line interface
      └── ...
```

## License

[MIT License](LICENSE) 
