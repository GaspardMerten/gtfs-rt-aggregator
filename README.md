# GTFS-RT Aggregator

This project provides a pipeline for fetching, storing, and aggregating GTFS-RT (General Transit Feed Specification -
Realtime) data from multiple providers into Parquet format.

## Features

- Fetch GTFS-RT data from multiple providers and APIs
- Store individual data files in Parquet format with multiple storage backends (filesystem, Google Cloud Storage, MinIO)
- Aggregate data files based on configurable time intervals
- Run fetcher and aggregator services in parallel
- Configurable via a single TOML configuration file

## Requirements

- Python 3.11+
- Required Python packages (see requirements.txt):
  - requests
  - gtfs-realtime-bindings
  - pandas
  - pyarrow
  - schedule
  - pydantic
  - google-cloud-storage (optional, for GCS storage)
  - minio (optional, for MinIO storage)

## Installation

### From PyPI (recommended)

```bash
pip install gtfs-rt-aggregator
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

  [[providers.apis]]
  url = "https://gtfs.ovapi.nl/nl/vehiclePositions.pb"
  services = ["VehiclePosition"]
  refresh_seconds = 20  # Fetch every 20 seconds
  frequency_minutes = 60  # Group files in 60-minute intervals
  check_interval_seconds = 300  # Check for new files every 5 minutes

  [[providers.apis]]
  url = "https://gtfs.ovapi.nl/nl/tripUpdates.pb"
  services = ["TripUpdate"]
  refresh_seconds = 20  # Fetch every 20 seconds
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
  - **params**: Backend-specific parameters

- **providers**: List of GTFS-RT data providers
  - **name**: Name of the provider (used for directory structure)
  - **timezone**: Timezone for the provider's data
  - **apis**: List of API endpoints for this provider
    - **url**: URL of the GTFS-RT feed
    - **services**: List of service types to extract from the feed (VehiclePosition, TripUpdate, Alert)
    - **refresh_seconds**: How often to fetch data from this API
    - **frequency_minutes**: The time interval (in minutes) for grouping files
    - **check_interval_seconds**: How often to check for new files to aggregate
    - **accumulate_minutes**: Keep fetches in memory and write them in blocks of this many minutes (default `0`: write every fetch right away). Blocks follow the clock in the provider's timezone: `15` gives 16:00-16:15, 16:15-16:30, and so on, and `1440` gives one block per day. The value must divide 1440 and `frequency_minutes`. A block is written when the next one starts, or when the pipeline stops cleanly. If the process is killed, the current block is lost. A whole block sits in memory, so use short blocks for large feeds.
    - **accumulate_concatenate**: Write each block as one Parquet file instead of one file per fetch (default `true`)

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
  ├── storage/                   # Storage backend implementations
  └── utils/                     # Utility functions and helpers
      ├── cli.py                 # Command-line interface
      └── ...
```

## License

[MIT License](LICENSE) 
