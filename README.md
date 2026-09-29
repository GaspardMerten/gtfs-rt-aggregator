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

- **runtime**: Optional, how the pipeline runs (see "How it runs" below)
  - **spool_dir**: Folder of downloads and results waiting to be processed or uploaded (default
    `<TMPDIR>/gtfs_rt_aggregator-spool`). Use a persistent disk: what is in it survives restarts.
  - **spool_max_gb**: Past this size, feeds stop being fetched, lowest `priority` first (default `10`)
  - **fetch_threads**: Downloads running at the same time (default `16`)
  - **workers**: Worker processes; `"auto"` is the number of CPUs this process may use (container limits and CPU
    affinity included) minus one, at least 1 (default `"auto"`)
  - **heavy_slots**: Worker processes for memory-heavy work: fetches larger than `heavy_threshold_mb`, static feeds,
    aggregation and compaction (default `1`)
  - **heavy_threshold_mb**: Fetches larger than this go to the heavy workers (default `8`)
  - **max_attempts**: Tries per fetch before it is moved to the spool's `quarantine/` folder (default `3`)
  - **startup_jitter_seconds**: Each job first runs at a random time within this delay (or its interval, if shorter),
    not all at once (default `60`)
  - **max_tasks_per_worker**: A worker process is replaced after this many tasks (default `200`)

- **raw**: Optional archive of the raw GTFS-RT fetches, to rebuild the Parquet files after a bug
  - **enabled**: Keep every fetch, bundled per feed and hour (default `false`)
  - **prefix**: Folder of the archive in each provider's storage (default `"raw"`)

- **output**: Optional, where aggregated files go
  - **path_template**: Path of each aggregated file. Fields: `provider`, `service`, `start` and `end` (the period, in
    the provider's timezone, with `strftime` formats). Default:
    `"provider={provider}/service={service}/date={start:%Y-%m-%d}/{start:%H-%M-%S}_to_{end:%H-%M-%S}.parquet"`
  - **compact_daily**: Merge the aggregated files of each finished day into one file (default `false`). Runs at
    startup, then every 24 hours, on the last 7 days. Needs one folder per day in `path_template`. Streams: files
    are sorted one at a time, then merged in batches. A day of 7.2 million rows (24 hourly files) peaks around 0.9 GB,
    against 3.5 GB when sorted in memory.
  - **compacted_name**: Name of that file, in the day's folder (default `"day.parquet"`)
  - **sort_by**: Columns the compacted file is sorted by (default `["entityId", "fetchTime"]`)
  - **trip_stop_events**: Write one row per trip and stop for each finished service date (default `false`, needs the
    `static` extra). See "Trip stop events".

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
    - **accumulate_minutes**: Write fetches in blocks of this many minutes, as one file per block (default `0`: one
      file per fetch). Blocks follow the clock in the provider's timezone: `15` gives 16:00-16:15, 16:15-16:30, and so
      on, and `1440` gives one block per day. The value must divide 1440 and `frequency_minutes`. A block is kept on
      disk in the spool until it ends, so it survives restarts and crashes.
    - **priority**: When the spool is full, feeds with the lowest priority stop being fetched first (default `0`)
    - **accumulate_concatenate**: No longer used (blocks are always one file)
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

- `provider` and `date`: the provider, and the local date of the fetch in its timezone
- `fetchTime`: when it was fetched (timestamp, UTC)
- `feedTimestamp`: the time in the feed's header (timestamp, UTC)
- `staticVersion`: the provider's static version current at that time, to join with the right timetable later
- `contentHash`: a hash of the entity, used to skip unchanged fetches and to deduplicate
- with `deduplicate = true`, after aggregation: `firstSeen` and `lastSeen` (timestamps, UTC)

Times inside the entities (e.g. a vehicle's `timestamp`, or `time` in a stop time update) stay Unix seconds, as in
GTFS-RT. No column is an unsigned integer, so the files can be read by engines without them (e.g. Iceberg).

**Unchanged fetches.** With `skip_unchanged` (the default), a fetch is not stored when its entities are exactly those of
the previous fetch, in any order, even if the header time changed. If you poll every 30 s and the feed changes every
2 min, only one fetch in four is stored.

**Deduplication.** With `deduplicate = true`, consecutive rows of the same entity with the same content become one row,
with `firstSeen` and `lastSeen` its first and last `fetchTime`. Polling every 30 s, a vehicle that stands still for ten
minutes is one row, not twenty. If the entity changes and then comes back to an earlier state, or is missing from a fetch in between, that
is a new row. Fetches skipped as unchanged are not counted, so `lastSeen` is the last *stored* fetch.

**Filter.** `[providers.realtime.filter]` keeps the rows matching any of:

- `route_types`: GTFS route types, as numbers or ranges, e.g. `[2, "100-199"]` for rail
- `route_ids`: route ids
- `trip_ids`: trip ids
- `keep_unmatched_added`: also keep ADDED, NEW and DUPLICATED trips whose route cannot be found in the static feed.
  Such trips are not in the timetable, so a route filter would otherwise drop them (default `false`; needs
  `route_types` or `route_ids`)

Realtime entities often carry only a trip id, so `route_types` and `route_ids` are resolved through the trips and
routes of the provider's latest static version (checked once a minute, every 5 seconds until the first version is stored): the provider needs a
`[[providers.static]]` feed. Trip updates and
vehicle positions are matched by trip or route (a vehicle without a trip is dropped), alerts by any informed route,
route type or trip; other entity types are kept. Until the first static version is stored, rows are not filtered.

**Status.** After each fetch, `<provider>/_status/<services>-<id>.json` holds the last attempt, last success, last
error, the feed's age (fetch time minus header time), the number of entities before and after the filter, and whether
the fetch was unchanged. The URL is stored without its query string, which may hold a key.

A period is aggregated once a file from the next period exists, or 5 minutes after it ended: a feed that stops
changing stores no new files. Files that arrive later are added to the period's file.

### How it runs

```
scheduler ─▶ fetch threads ─▶ spool/incoming ─▶ worker processes ─▶ spool/ready ─▶ upload thread ─▶ storage
```

- **Fetch threads** only download, streaming each fetch to a file in the spool (`runtime.spool_dir`): a fetch is
  never held in memory and never waits for processing. With `skip_unchanged` (the default), a fetch identical, byte
  for byte, to the previous one is dropped right away.
- **Worker processes** (a fixed pool, `runtime.workers`) parse, filter and convert each fetch to Parquet, one at a time
  per feed and in fetch order. Memory-heavy work goes to a separate pool (`runtime.heavy_slots`, 1 by default): large
  fetches, static feeds, aggregation and compaction: with the default `heavy_slots = 1`, two of them never run at once. Each static feed is converted in a
  new process, which gives all its memory back when done.
- **The upload thread** moves results to storage, and checks they arrived. While storage is down, results wait on
  disk.
- A fetch whose processing fails (or whose worker is killed, e.g. out of memory) is retried, and moved to the spool's
  `quarantine/` folder after `runtime.max_attempts`. On restart, whatever was in progress is picked up again.
- When the spool grows past `runtime.spool_max_gb`, feeds stop being fetched, lowest `priority` first, until it is
  below 90 % again. Nothing already on disk is dropped.
- `_status/index.json` in the global storage summarises every feed: last success and error, feed age, fetches
  waiting and the age of the oldest, spool size. Each worker logs its peak memory.

Converting protobuf to Parquet builds Arrow columns straight from the protobuf messages, without an intermediate
dict per message: a feed of 77,000 trip updates (28 MB) takes a few seconds.

### Storage Layout

```
ovapi/VehiclePosition/individual/2026-09-28_14-00-20Z.parquet                        # one fetch (or block), UTC
provider=ovapi/service=VehiclePosition/date=2026-09-28/16-00-00_to_17-00-00.parquet  # aggregated, local time
provider=ovapi/service=VehiclePosition/date=2026-09-27/day.parquet                   # compacted day
ovapi/raw/provider=ovapi/feed=VehiclePosition-1a2b3c4d/date=2026-09-28/14-1790000000.tar.zst  # raw archive (optional)
ovapi/static/2026-09-28_01-00-00Z/stops.parquet                 # one static version (UTC), one file per table
ovapi/static/2026-09-28_01-00-00Z/manifest.json
ovapi/static/latest.json                                        # manifest of the latest version
ovapi/_status/VehiclePosition-1a2b3c4d.json                     # status of a realtime feed
```

Aggregated files are in Hive-style folders (`key=value`), so DuckDB, Polars or BigQuery can read them as one table and
skip folders when filtering on provider, service or date:

```sql
SELECT * FROM read_parquet('data/provider=*/service=VehiclePosition/date=*/*.parquet',
                            hive_partitioning = true, union_by_name = true)
WHERE date = '2026-09-28'
```

Individual files and static versions are named after their fetch time in UTC (the `Z` suffix), so the hour that repeats when clocks go back never produces the same name twice. Aggregation periods, daily folders and aggregated files use the provider's timezone; on the night clocks go back, the repeated hour ends up in a single aggregated file covering both passes.

A new static version is stored only when a file inside the zip changed. The pipeline first asks the server whether the feed changed since the last check (ETag / Last-Modified), then compares the checksum and size of each file in the zip. A zip rebuilt with the same files is not stored again. Each version is a full copy of the feed (unless `reuse_unchanged_tables` is set: `manifest.json` then gives the path of each table), converted table by table with gtfs-parquet's `convert_gtfs_zip`, so the feed is never fully in memory: the German national feed (a 298 MB zip, 40 million stop times) takes about a minute and 0.5 GB of RAM. Static jobs limit Polars to 4 threads, since its memory grows with the thread count; set `POLARS_MAX_THREADS` to change it. Rows keep the order of the source files. The zip and the converted tables are kept in a temporary folder until uploaded (about 1.5 GB for the German feed): if `/tmp` is in RAM (tmpfs, common in containers), point `TMPDIR` to a disk. If a check is still running when the next one is due, the next one is skipped.

### Iceberg tables (optional)

With `pip install "gtfs-rt-aggregator[iceberg]"` and an `[iceberg]` section, the compacted days are also exposed as
Iceberg tables, one per service type, readable by DuckDB, Polars, Spark or Trino:

```toml
[output]
compact_daily = true  # required: the tables are made of the compacted days

[iceberg]
catalog = "sql"                                     # "sql" (a SQLite file, no service) or "rest"
catalog_uri = "sqlite:////var/lib/gtfs/iceberg.db"  # or the REST catalog URL
warehouse = "iceberg"                               # folder of the tables in the global storage
namespace = "archive"
services = ["TripUpdate", "VehiclePosition"]
write_version_hint = true                           # lets readers without a catalog find the tables
expire_snapshots_days = 7
sync_minutes = 60                                   # how often new compacted days are registered
```

- The day files are registered as they are (no copy): a table adds no storage besides its metadata. A day compacted
  again (late files) replaces its earlier file in the same commit.
- Tables are partitioned by `provider` and `date` (the local date, as the files are).
- The storage settings (MinIO/S3, GCS or filesystem) are reused to read and write the tables. Providers with their own
  storage are not registered.
- Every week, snapshots older than `expire_snapshots_days` are expired.
- To register the days compacted before the tables existed (and convert files written before 0.6.0), run once:
  `gtfs-rt-pipeline configuration.toml --iceberg-backfill`.

Reading without a catalog, from the metadata the tables keep in storage:

```sql
INSTALL iceberg; LOAD iceberg;
SELECT provider, date, count(*) FROM iceberg_scan('s3://my-bucket/iceberg/TripUpdate') GROUP BY ALL;
```

```python
import polars as pl
pl.scan_iceberg("s3://my-bucket/iceberg/TripUpdate/metadata/<latest>.metadata.json")
```

### Trip stop events

With `trip_stop_events = true` in `[output]`, every provider with trip updates and a static feed gets a daily
`TripStopEvent` file, in the `path_template` folder of the service date (e.g.
`provider=be/service=TripStopEvent/date=2026-10-24/day.parquet`). One row per trip and stop of that service date:

| Column | Meaning |
|---|---|
| `provider`, `service_date`, `date` | `date` equals `service_date` (same partition column as the other services) |
| `trip_id`, `route_id`, `trip_start_time` | from the trip descriptor, the route from the timetable if missing |
| `stop_sequence`, `stop_id` | updates matched to the timetable by `stop_sequence`, else by `stop_id` |
| `scheduled_arrival`, `scheduled_departure` | from the static `stop_times`, as Unix seconds (right past 24:00 and on DST days) |
| `first_arrival_delay`, `last_arrival_delay`, `first_departure_delay`, `last_departure_delay` | first and last prediction, in seconds |
| `last_predicted_arrival`, `last_predicted_departure` | last predicted times, as Unix seconds |
| `first_seen`, `last_seen`, `prediction_count` | when the stop's predictions were made, how many |
| `observed` | the last prediction was made after the predicted time (or the stop left the feed while the trip stayed in it) |
| `delay_propagated` | the stop had no update of its own: the delay comes from the stop before |
| `trip_schedule_relationship`, `stop_schedule_relationship` | e.g. `CANCELED`, `ADDED`; `SKIPPED`, `NO_DATA` |
| `static_version` | static version used (the one the updates were fetched with) |

- A service date is built once, after D+1 is over and aggregated, from the trip updates of the local days D-1, D and
  D+1 (predictions made the evening before, trips past midnight). Each run builds one day, in a process of its own.
- Delays only (e.g. SNCB) or times only (e.g. Entur) both work: the missing one is computed from the schedule.
- Trips without `startDate` are assigned to the day whose scheduled run contains their updates; trips without
  `trip_id`, by route and start time. Runs of frequency-based trips are told apart by start time. Canceled trips keep their scheduled stops, without times. Added trips unknown to
  the timetable keep their updates, without a schedule.
- To build the days before it was enabled: `gtfs-rt-pipeline configuration.toml --trip-stop-events-backfill 30`.
- With Iceberg, add `"TripStopEvent"` to `iceberg.services`.

```sql
-- Average arrival delay per route, from the last prediction of observed stops
SELECT route_id, avg(last_arrival_delay) / 60 AS minutes
FROM read_parquet('out/provider=be/service=TripStopEvent/date=2026-10-24/day.parquet')
WHERE observed GROUP BY ALL ORDER BY minutes DESC;
```

### Upgrading to 0.7.0

- Optional `TripStopEvent` daily files (see "Trip stop events").

### Upgrading to 0.6.1

- Optional Iceberg tables (see above), with the `iceberg` extra.

### Upgrading to 0.6.0

- The pipeline runs with a disk spool, a fixed pool of worker processes and an upload thread, instead of one process
  per job (see "How it runs"). Set `runtime.spool_dir` to a persistent disk. Passing a `SchedulerClass` to
  `GtfsRtPipeline` still selects the old way of running, which is deprecated.
- Realtime files are ready for Iceberg and other engines without unsigned integers:
  - `fetchTime`, `feedTimestamp`, `firstSeen` and `lastSeen` are timestamps (microseconds, UTC) instead of Unix
    seconds. Times inside the entities (e.g. a vehicle's `timestamp`) stay Unix seconds, as in GTFS-RT.
  - Unsigned integer columns are now signed 64-bit integers.
  - Every row has `provider` and `date` (the local date of `fetchTime`) columns.
  - Alerts' `severityLevel` is the enum name (e.g. `WARNING`), like other enums: alerts with a severity used to fail.

  Files already stored keep their old types; only the files the aggregator rewrites are converted. DuckDB, Polars and
  other engines cannot read old and new files as one table, so convert the old ones once:
  `gtfs-rt-pipeline configuration.toml --convert-old-files` (each file is read whole).
- `accumulate_minutes` blocks live on disk in the spool instead of in memory; `accumulate_concatenate` is ignored.
- `FetcherService.run_once` stores each fetch right away (no accumulation); the runtime does the rest.
- New options: `[runtime]`, `[raw]`, per-feed `priority`, and `filter.keep_unmatched_added`.

### Upgrading to 0.5.1

- Static manifests (`manifest.json`, `latest.json`) no longer save the query string of the feed's URL, which may hold
  an API key. To clean manifests written by earlier versions, run once:
  `gtfs-rt-pipeline configuration.toml --scrub-urls`. If your storage is public, also rotate those keys.
- SIGTERM (systemd, Docker, Kubernetes) now stops the pipeline like Ctrl+C: buffered data is written before exiting.
- At startup, temporary folders left by killed jobs are deleted.
- Each job logs its duration and peak memory when it ends.

### Upgrading to 0.5.0

- Aggregated files now go to Hive-style folders (`provider=…/service=…/date=…/`). Set `path_template` to keep the old
  layout: `"{provider}/{service}/{start:%Y-%m-%d}/{start:%H-%M-%S}_to_{end:%H-%M-%S}.parquet"`. `filename_format` and
  `time_format` still work but are deprecated.
- Fetches whose entities did not change are no longer stored (`skip_unchanged = false` to keep every fetch).
- New columns: `feedTimestamp`, `staticVersion` and `contentHash`.
- `manifest.json` gives each table's path in `tables` (it was a list of names).
- Static feeds need gtfs-parquet 0.6.1 or later. Earlier versions stored extended route types of 128 and more (e.g.
  700 for buses) as empty values; versions stored with them keep that until the feed changes.
- Unknown options in providers, `[[providers.realtime]]` and `[output]` are now rejected.
- A status file per realtime feed is written to `<provider>/_status/`.
- The pipeline now always starts a `multiprocessing` Manager, whose socket lives in the temporary folder: keep
  `TMPDIR` short (a long path fails with "AF_UNIX path too long").

### Upgrading to 0.4.0

- Static feeds use much less memory (see above).
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

Register every compacted day in the Iceberg tables (converting files stored before 0.6.0 first), then exit:

```bash
gtfs-rt-pipeline configuration.toml --iceberg-backfill
```

Build the missing `TripStopEvent` days among the last 30 days, then exit:

```bash
gtfs-rt-pipeline configuration.toml --trip-stop-events-backfill 30
```

Rewrite aggregated files stored before 0.6.0 with the current types, then exit:

```bash
gtfs-rt-pipeline configuration.toml --convert-old-files
```

Remove API keys from the URLs saved in static manifests by versions before 0.5.1, then exit:

```bash
gtfs-rt-pipeline configuration.toml --scrub-urls
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
  ├── runtime/                   # Spool, fetch threads, worker processes, upload thread
  ├── sinks/                     # Optional Iceberg tables
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
