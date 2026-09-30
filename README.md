# GTFS-RT Aggregator

Archives GTFS-RT realtime feeds to Parquet. It fetches each feed on a schedule, groups the fetches into hourly (or
other) files, and can compact each day into one sorted file. It can also keep a Parquet copy of every version of a
provider's GTFS static feed, expose the compacted days as Iceberg tables, and build a daily `TripStopEvent` table (one
row per trip and stop).

Storage can be a local folder, Google Cloud Storage or MinIO. Everything is configured in one TOML file.

## Installation

Python 3.11 or later.

```bash
pip install gtfs-rt-aggregator             # realtime feeds only
pip install "gtfs-rt-aggregator[static]"   # also GTFS static feeds and TripStopEvent
pip install "gtfs-rt-aggregator[iceberg]"  # also Iceberg tables
```

From source:

```bash
git clone https://github.com/GaspardMerten/gtfs-rt-aggregator
cd gtfs-rt-aggregator
pip install -e .
```

## Quick start

```toml
[storage]
type = "filesystem"
[storage.params]
base_directory = "data"

[[providers]]
name = "ovapi"
timezone = "Europe/Amsterdam"

  [[providers.realtime]]
  url = "https://gtfs.ovapi.nl/nl/vehiclePositions.pb"
  services = ["VehiclePosition"]
  refresh_seconds = 20    # fetch every 20 s
  frequency_minutes = 60  # one aggregated file per hour
  deduplicate = true      # merge identical consecutive rows of a vehicle

  [[providers.realtime]]
  url = "https://gtfs.ovapi.nl/nl/tripUpdates.pb"
  services = ["TripUpdate"]
  refresh_seconds = 20
  [providers.realtime.filter]
  route_types = [2, "100-199"]  # rail only, resolved through the static feed below

  [[providers.static]]
  url = "https://gtfs.ovapi.nl/nl/gtfs-nl.zip"
  check_minutes = 60

[[providers]]
name = "uk"
timezone = "Europe/London"

  [[providers.realtime]]
  url = "https://example.org/uk/gtfsrt.pb"
  services = ["VehiclePosition"]
  [providers.realtime.headers]
  x-api-key = "${UK_API_KEY}"  # read from the environment
```

```bash
gtfs-rt-pipeline configuration.toml
```

Each job first runs within a minute of startup (see `startup_jitter_seconds`), then at its interval. The pipeline
stops cleanly on Ctrl+C or SIGTERM.

Any string in the file can read an environment variable with `${NAME}`. Loading fails if the variable is not set.
Write `$${` for a literal `${`.

## Configuration reference

Unknown keys in providers, feeds, `[output]`, `[runtime]`, `[raw]` and `[iceberg]` are rejected, so typos fail at
load time.

### `[storage]`

`type` is `filesystem`, `gcs` or `minio`. Parameters go in `[storage.params]`:

| Type | Parameters |
|---|---|
| `filesystem` | `base_directory` (default `.`) |
| `gcs` | `bucket_name` (required), `base_path`. Credentials come from `GOOGLE_APPLICATION_CREDENTIALS`. |
| `minio` | `endpoint`, `access_key`, `secret_key`, `bucket_name` (all required), `secure` (default `true`), `base_path` |

```toml
[storage]
type = "minio"
[storage.params]
endpoint = "minio.example.com:9000"
access_key = "${MINIO_ACCESS_KEY}"
secret_key = "${MINIO_SECRET_KEY}"
bucket_name = "gtfs-data"
```

A provider can have its own `[providers.storage]` with the same fields.

### `[[providers]]`

| Key | Default | Meaning |
|---|---|---|
| `name` | required | Unique. Used in paths. |
| `timezone` | `"UTC"` | Used for aggregation periods and daily folders. |
| `frequency_minutes`, `check_interval_seconds` | | Defaults for this provider's realtime feeds. |
| `storage` | global storage | Storage for this provider only. |

A provider needs at least one realtime or static feed.

### `[[providers.realtime]]`

| Key | Default | Meaning |
|---|---|---|
| `url` | required | Feed URL. |
| `adapter` | | Instead of `url`: a Python function that returns each fetch. See [Adapters](#adapters). |
| `services` | required | Any of `VehiclePosition`, `TripUpdate`, `Alert`, `TripModifications`. |
| `refresh_seconds` | `60` | How often to fetch. |
| `frequency_minutes` | `60` | Length of an aggregation period. |
| `check_interval_seconds` | `300` | How often to look for periods ready to aggregate. |
| `accumulate_minutes` | `0` | Write fetches in blocks of this many minutes, one file per block, aligned on the local clock. `0` writes one file per fetch. Must divide 1440 and `frequency_minutes`. |
| `headers` | none | HTTP headers, e.g. an API key. |
| `retries` | `3` | Retries on connection errors, timeouts and HTTP 429/5xx, with backoff. |
| `skip_unchanged` | `true` | Do not store a fetch whose entities are the same as the previous fetch's, in any order. |
| `deduplicate` | `false` | When aggregating, merge consecutive identical rows of an entity into one row with `firstSeen` and `lastSeen`. |
| `filter` | keep all | See below. |
| `static` | | Name of the static feed to use, if the provider has several. |
| `priority` | `0` | When the spool is full, feeds with the lowest priority stop being fetched first. |

`[[providers.apis]]` is accepted as an old name for `[[providers.realtime]]`.

A provider can have several feeds of the same service, e.g. one per operator. Their rows go to the same aggregated
files, and `feedId` tells them apart; deduplication is done feed by feed. Feeds of the same service must have the same
`frequency_minutes` and `deduplicate`.

With `deduplicate = true`, a vehicle standing still for ten minutes, polled every 30 s, is one row instead of twenty.
If the entity changes and later returns to an earlier state, that is a new row. So is an entity that was missing from a
fetch in between. With `skip_unchanged = true`, a fetch identical to the previous one is not stored: `lastSeen` is then
the last stored fetch that had the entity, which can be earlier than the last time the feed showed it.

#### Filter

`[providers.realtime.filter]` keeps rows matching any of:

- `route_types`: GTFS route types, as numbers or ranges, e.g. `[2, "100-199"]` for rail
- `route_ids`: route ids
- `trip_ids`: trip ids
- `keep_unmatched_added`: also keep ADDED, NEW and DUPLICATED trips whose route is not in the static feed (default
  `false`, needs `route_types` or `route_ids`)

`route_types` and `route_ids` are resolved through the trips and routes of the provider's latest static version, so the
provider needs a `[[providers.static]]` feed. Until a static version can be read (the first one is being downloaded,
or storage is down), fetches wait in the spool; after 3 hours they are dropped, never stored unfiltered. Trip
updates and vehicle positions match by trip or route (vehicles without a trip are dropped). Alerts match by any
informed route, route type or trip. Other entity types are always kept.

### `[[providers.static]]`

Needs the `static` extra. A new version is stored only when a file inside the zip changed.

| Key | Default | Meaning |
|---|---|---|
| `url` | | URL of the GTFS zip. |
| `index_url`, `url_pattern` | | Instead of `url`, for feeds published under a new URL for each version. The page at `index_url` (HTML or JSON) is read and the greatest link matching the regex `url_pattern` is downloaded. |
| `adapter` | | Instead of `url`: a Python function that builds the GTFS. See [Adapters](#adapters). |
| `check_minutes` | `60` | How often to check for a new version. |
| `name` | `"static"` | Folder the versions are stored in. Only needed if the provider has several static feeds. |
| `headers` | none | HTTP headers. |
| `retries` | `3` | As for realtime feeds. |
| `reuse_unchanged_tables` | `false` | Point to the previous version's file for tables that did not change, instead of storing them again. `manifest.json` then gives each table's path. |

```toml
[[providers.static]]
index_url = "https://data.public.lu/api/1/datasets/horaires-et-arrets-des-transport-publics-gtfs/"
url_pattern = 'https://download\.data\.public\.lu/resources/horaires-et-arrets-des-transport-publics-gtfs/\d{8}-\d{6}/gtfs-[\d-]+\.zip'
check_minutes = 1440
```

Static conversion writes temporary files (about 1.5 GB for a large national feed). If `/tmp` is in RAM, set `TMPDIR`
to a folder on disk.

### Adapters

For a source that is not a GTFS-RT or GTFS URL, such as a JSON API, write a Python function and name it with `adapter`
instead of `url`: `"path/to/file.py:function"` (relative to the configuration file) or `"module:function"`.

```toml
[[providers.realtime]]
adapter = "adapters/trafikverket.py:fetch"
services = ["TripUpdate"]
refresh_seconds = 30

[[providers.static]]
adapter = "adapters/trafikverket.py:timetable"
check_minutes = 1440
```

```python
def fetch(state: dict, env) -> FeedMessage | bytes: ...          # one poll, as GTFS-RT
def timetable(state: dict, env, out_dir: Path) -> Path: ...     # a GTFS zip or folder, written in out_dir
```

What they return is handled like a download: filter, deduplication, versions, and so on. `state` is the adapter's
own dict (JSON values only). It is kept between calls and across restarts, and saved only after a call that succeeds.
Use it for things like a change id. `env` is `os.environ`, for API keys. Adapters run in the worker processes, and a
file is loaded again when it changes. An adapter that raises counts as a failed fetch, and it is called again at the
next refresh. `feedId` is computed from the `adapter` text as written. After a restart, a static adapter is not
called again before `check_minutes` if a version is already stored.

### `[output]`

| Key | Default | Meaning |
|---|---|---|
| `path_template` | `"provider={provider}/service={service}/date={start:%Y-%m-%d}/{start:%H-%M-%S}_to_{end:%H-%M-%S}.parquet"` | Path of each aggregated file. Fields: `provider`, `service`, `start`, `end` (period bounds in the provider's timezone, with `strftime` formats). |
| `compact_daily` | `false` | Merge the aggregated files of each finished day into one sorted file. Checks every hour over the last 7 days: a day is compacted once its last period is aggregated, and again when files are added to it. |
| `compacted_name` | `"day.parquet"` | Name of that file in the day's folder. |
| `sort_by` | `["entityId", "fetchTime"]` | Sort order of the aggregated and compacted files. |
| `trip_stop_events` | `false` | Build a daily `TripStopEvent` file. Needs the `static` extra. See below. |

`compact_daily` and `trip_stop_events` need a `path_template` with one folder per day and `{service}` in a folder name.

### `[runtime]`

Fetches are downloaded to a spool folder on disk, converted by worker processes, then uploaded. Anything in the spool
is picked up again after a restart, and waits there while storage is down.

| Key | Default | Meaning |
|---|---|---|
| `spool_dir` | `<TMPDIR>/gtfs_rt_aggregator-spool` | Spool folder. Use a persistent disk. One pipeline per folder. |
| `spool_max_gb` | `10` | Past this size (`quarantine/` included), feeds stop being fetched, lowest `priority` first. Nothing on disk is dropped. |
| `fetch_threads` | `16` | Parallel downloads. |
| `workers` | `"auto"` | Worker processes. `"auto"` is the available CPUs minus one, at least 1. |
| `heavy_slots` | `1` | Worker processes for memory-heavy work (large fetches, static feeds, aggregation, compaction). |
| `heavy_threshold_mb` | `8` | Fetches larger than this go to the heavy workers. |
| `max_attempts` | `3` | Tries per fetch before it is moved to the spool's `quarantine/` folder. A fetch that is not valid GTFS-RT goes there at once. |
| `startup_jitter_seconds` | `60` | Each job first runs at a random time within this delay, so they do not all start at once. |
| `max_tasks_per_worker` | `200` | A worker process is replaced after this many tasks. |

### `[raw]`

Keeps every raw fetch, bundled per feed and hour, so the Parquet files can be rebuilt later.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Turn the archive on. |
| `prefix` | `"raw"` | Folder of the archive in each provider's storage. |

### `[iceberg]`

Needs the `iceberg` extra and `compact_daily = true`. The compacted days are registered as they are (no copy) in one
Iceberg table per service type, partitioned by `provider` and `date`. The tables use the global storage; providers
with their own storage are not registered.

```toml
[iceberg]
catalog = "sql"
catalog_uri = "sqlite:////var/lib/gtfs/iceberg.db"
services = ["TripUpdate", "VehiclePosition"]
```

| Key | Default | Meaning |
|---|---|---|
| `catalog` | `"sql"` | `"sql"` (a SQLite file, no service) or `"rest"`. |
| `catalog_uri` | required | `sqlite:////path/catalog.db`, or the REST catalog URL. |
| `warehouse` | `"iceberg"` | Folder of the tables in the global storage. |
| `namespace` | `"archive"` | Iceberg namespace. |
| `services` | `["TripUpdate", "VehiclePosition"]` | One table per service. `TripStopEvent` is allowed. |
| `write_version_hint` | `true` | Write `metadata/version-hint.text`, so readers without a catalog find the tables. |
| `expire_snapshots_days` | `7` | Snapshots older than this are expired, once a week. |
| `sync_minutes` | `60` | How often new compacted days are registered. |
| `public_base_url` | none | URL under which a web server or CDN serves the storage's objects by key. When set, a copy of each table's metadata with every path rewritten to `<public_base_url>/<key>` is kept in `public_warehouse`. |
| `public_warehouse` | `"<warehouse>-public"` | Folder of that public copy. Must differ from `warehouse`. |

## Command line

```bash
gtfs-rt-pipeline configuration.toml [options]
```

Without options, the pipeline runs until stopped. Each of the other options does one job and exits. The exit status is
1 on error.

| Option | Effect |
|---|---|
| `--log-level LEVEL` | Logging level (default `INFO`). |
| `--iceberg-backfill` | Convert files written before 0.6.0, compact older days that only have hourly files, then register every compacted day in Iceberg. Safe to run next to the pipeline. |
| `--iceberg-publish` | Write the public metadata copy (`public_base_url`) again for every table, e.g. after changing the URL. |
| `--trip-stop-events-backfill DAYS` | Build the missing `TripStopEvent` days among the last `DAYS` days. |
| `--convert-old-files` | Rewrite aggregated files written before 0.6.0 with the current column types. |
| `--scrub-urls` | Remove query strings (which may hold API keys) from URLs in static manifests written before 0.5.1. |

From Python:

```python
from gtfs_rt_aggregator import run_pipeline_from_toml

run_pipeline_from_toml("configuration.toml")
```

## Output

### Layout

```
ovapi/VehiclePosition/individual/2026-09-28_14-00-20Z-1a2b3c4d.parquet               # one fetch or block (UTC, feed id)
provider=ovapi/service=VehiclePosition/date=2026-09-28/16-00-00_to_17-00-00.parquet  # aggregated period (local time)
provider=ovapi/service=VehiclePosition/date=2026-09-27/day.parquet                   # compacted day
ovapi/raw/provider=ovapi/feed=VehiclePosition-1a2b3c4d/date=2026-09-28/14-1790000000.tar.zst  # raw archive
ovapi/static/2026-09-28_01-00-00Z/stops.parquet    # one static version, one file per table
ovapi/static/2026-09-28_01-00-00Z/manifest.json
ovapi/static/latest.json                           # manifest of the latest version
ovapi/_status/VehiclePosition-1a2b3c4d.json        # status of one realtime feed
_status/index.json                                 # summary of all feeds (global storage)
```

A period is aggregated once a file from the next period exists, or 5 minutes after it ended. Files that arrive later
are added to the period's file.

Each feed's status file holds the last attempt, last success and last error, the feed's age, and the entity count
before and after the filter. URLs are stored without their query string.

### Realtime columns

Each row has the entity's fields plus:

- `provider`, `date`: the provider, and the local date of the fetch
- `fetchTime`: fetch time (timestamp, UTC)
- `feedTimestamp`: time in the feed header (timestamp, UTC)
- `staticVersion`: the provider's static version at fetch time, to join with the right timetable
- `feedId`: 8 characters identifying the realtime feed the row comes from
- `contentHash`: hash of the entity
- `firstSeen`, `lastSeen`: with `deduplicate = true`, after aggregation (timestamps, UTC)

Times inside entities, such as a vehicle's `timestamp`, stay Unix seconds as in GTFS-RT. There are no unsigned integer
columns.

### Reading with DuckDB

```sql
SELECT *
FROM read_parquet('data/provider=*/service=VehiclePosition/date=*/*.parquet',
                  hive_partitioning = true, union_by_name = true)
WHERE date = '2026-09-28';
```

Iceberg tables, without a catalog:

```sql
INSTALL iceberg; LOAD iceberg;
SELECT provider, date, count(*) FROM iceberg_scan('s3://my-bucket/iceberg/TripUpdate') GROUP BY ALL;
```

Or over https, with `public_base_url` set:

```sql
-- If the server needs a token: CREATE SECRET (TYPE http, BEARER_TOKEN '...');
SELECT count(*) FROM iceberg_scan('https://data.example.org/iceberg-public/TripUpdate') WHERE provider = 'be';
```

### TripStopEvent

With `trip_stop_events = true`, each provider with trip updates and a static feed gets one file per service date, e.g.
`provider=be/service=TripStopEvent/date=2026-10-24/day.parquet`. It has one row per trip and stop:

| Column | Meaning |
|---|---|
| `provider`, `service_date`, `date` | `date` equals `service_date` |
| `trip_id`, `route_id`, `trip_start_time` | From the trip descriptor. `route_id` from the timetable if missing. |
| `stop_sequence`, `stop_id` | Updates are matched to the timetable by `stop_sequence`, else by `stop_id`. |
| `scheduled_arrival`, `scheduled_departure` | From static `stop_times`, as Unix seconds |
| `first_arrival_delay`, `last_arrival_delay`, `first_departure_delay`, `last_departure_delay` | First and last prediction, in seconds |
| `last_predicted_arrival`, `last_predicted_departure` | Last predicted times, as Unix seconds |
| `first_seen`, `last_seen`, `prediction_count` | When predictions for this stop were made, and how many |
| `observed` | The last prediction was made after the predicted time, or the stop left the feed while the trip stayed |
| `delay_propagated` | The stop had no update of its own; the delay comes from the previous stop |
| `trip_schedule_relationship`, `stop_schedule_relationship` | e.g. `CANCELED`, `ADDED`, `SKIPPED`, `NO_DATA` |
| `static_version` | Static version the updates were fetched with |

A service date D is built once D+1 is over and aggregated (and compacted, with `compact_daily`), from the trip updates
of D-1, D and D+1. It is built again when those trip updates change, e.g. late files. Feeds that give
only delays or only times both work; the missing value is computed from the schedule. Canceled trips keep their
scheduled stops without times. Added trips that are not in the timetable keep their updates without a schedule.

```sql
-- Average arrival delay per route, from the last prediction of observed stops
SELECT route_id, avg(last_arrival_delay) / 60 AS minutes
FROM read_parquet('data/provider=be/service=TripStopEvent/date=2026-10-24/day.parquet')
WHERE observed
GROUP BY ALL
ORDER BY minutes DESC;
```

## License

[MIT](LICENSE)
