# GTFS-RT Aggregator

This project provides a pipeline for fetching, storing, and aggregating GTFS-RT (General Transit Feed Specification -
Realtime) data from multiple providers into Parquet format. It can also keep a Parquet copy of each version of the
providers' GTFS static feeds.

## Features

- Fetch GTFS-RT data from multiple providers and APIs
- Store a new copy of each GTFS static feed when it changes (optional)
- Store individual data files in Parquet format with multiple storage backends (filesystem, Google Cloud Storage, MinIO)
- Aggregate data files based on configurable time intervals
- Run fetcher and aggregator services in parallel
- Configurable via a single TOML configuration file

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
# GTFS-RT Configuration File
[storage]
type = "filesystem"  # Options: "filesystem", "gcs", or "minio"
[storage.params]
base_directory = "data"  # Base directory for filesystem storage

# Provider configurations
[[providers]]
name = "ovapi"
timezone = "Europe/Amsterdam"

  [[providers.realtime]]
  url = "https://gtfs.ovapi.nl/nl/vehiclePositions.pb"
  services = ["VehiclePosition"]
  refresh_seconds = 20  # Fetch every 20 seconds
  frequency_minutes = 60  # Group files in 60-minute intervals
  check_interval_seconds = 300  # Check for new files every 5 minutes

  [[providers.realtime]]
  url = "https://gtfs.ovapi.nl/nl/tripUpdates.pb"
  services = ["TripUpdate"]
  refresh_seconds = 20  # Fetch every 20 seconds

  [[providers.static]]
  url = "https://gtfs.ovapi.nl/nl/gtfs-nl.zip"
  check_minutes = 60  # Check for a new version every hour

[[providers]]
name = "uk"
timezone = "Europe/London"

  [[providers.realtime]]
  url = "https://example.org/uk/gtfsrt.pb"
  services = ["VehiclePosition"]
  [providers.realtime.headers]
  x-api-key = "YOUR_API_KEY"
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

- **output**: Optional naming of aggregated files
  - **filename_format**: Default `"{group_time}_to_{next_period}.parquet"`
  - **time_format**: `strftime` format of `group_time` and `next_period` (default `"%H-%M-%S"`)

- **providers**: List of data providers
  - **name**: Name of the provider (used for directory structure, must be unique)
  - **timezone**: Timezone for the provider's data (default `"UTC"`)
  - **storage**: Optional storage for this provider only (same fields as the global one)
  - **realtime**: List of GTFS-RT feeds for this provider (called `apis` before 0.3.0, which still works)
    - **url**: URL of the GTFS-RT feed
    - **headers**: Optional HTTP headers sent with each request, e.g. an API key
    - **services**: List of service types to extract from the feed (VehiclePosition, TripUpdate, Alert, TripModifications)
    - **refresh_seconds**: How often to fetch data from this API (default `60`)
    - **frequency_minutes**: The time interval (in minutes) for grouping files (default `60`)
    - **check_interval_seconds**: How often to check for new files to aggregate (default `300`)
    - **accumulate_minutes**: Keep fetches in memory and write them in blocks of this many minutes (default `0`: write every fetch right away). Blocks follow the clock in the provider's timezone: `15` gives 16:00-16:15, 16:15-16:30, and so on, and `1440` gives one block per day. The value must divide 1440 and `frequency_minutes`. A block is written when the next one starts, or when the pipeline stops cleanly. If the process is killed, the current block is lost. A whole block sits in memory, so use short blocks for large feeds.
    - **accumulate_concatenate**: Write each block as one Parquet file instead of one file per fetch (default `true`)
  - **static**: List of GTFS static feeds for this provider (needs the `static` extra)
    - **url**: URL of the GTFS zip
    - **check_minutes**: How often to check for a new version (default `60`)
    - **name**: Folder the versions are stored in (default `static`). Only needed when a provider has several static feeds.
    - **headers**: Optional HTTP headers sent with each request

A provider can have realtime feeds, static feeds, or both. Every job runs once at startup, then at its interval.

### Storage Layout

```
ovapi/VehiclePosition/individual/2026-09-28_14-00-20Z.parquet   # one fetch (or one accumulated block), UTC
ovapi/VehiclePosition/2026-09-28/16-00-00_to_17-00-00.parquet   # aggregated, local time
ovapi/static/2026-09-28_01-00-00Z/stops.parquet                 # one static version (UTC), one file per table
ovapi/static/2026-09-28_01-00-00Z/manifest.json
ovapi/static/latest.json                                        # manifest of the latest version
```

Individual files and static versions are named after their fetch time in UTC (the `Z` suffix), so the hour that repeats when clocks go back never produces the same name twice. Aggregation periods, daily folders and aggregated files use the provider's timezone; on the night clocks go back, the repeated hour ends up in a single aggregated file covering both passes. Every row keeps its own `fetchTime` (Unix time).

A new static version is stored only when a file inside the zip changed. The pipeline first asks the server whether the feed changed since the last check (ETag / Last-Modified), then compares the checksum and size of each file in the zip. A zip rebuilt with the same files is not stored again. Each version is a full copy of the feed, converted table by table with gtfs-parquet's `convert_gtfs_zip`, so the feed is never fully in memory: the German national feed (a 298 MB zip, 40 million stop times) takes about a minute and 0.5 GB of RAM. Static jobs limit Polars to 4 threads, since its memory grows with the thread count; set `POLARS_MAX_THREADS` to change it. Rows keep the order of the source files. The zip and the converted tables are kept in a temporary folder until uploaded (about 1.5 GB for the German feed): if `/tmp` is in RAM (tmpfs, common in containers), point `TMPDIR` to a disk. If a check is still running when the next one is due, the next one is skipped.

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
