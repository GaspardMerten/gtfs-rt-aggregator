# GTFS-RT Pipeline

This project provides a pipeline for fetching, storing, and aggregating GTFS-RT (General Transit Feed Specification -
Realtime) data from multiple providers.

## Features

- Fetch GTFS-RT data from multiple providers and APIs
- Store individual data files in Parquet format
- Aggregate data files based on configurable time intervals
- Run multiple fetchers in parallel using separate processes
- Configurable via a single TOML configuration file

## Requirements

- Python 3.7+
- Required Python packages:
  - pandas
  - pytz
  - requests
  - protobuf
  - toml

## Installation

1. Clone this repository:
   ```
   git clone <repository-url>
   cd gtfs-rt-aggregator
   ```

2. Install the required dependencies:
   ```
   pip install pandas pytz requests protobuf toml
   ```

## Configuration

The pipeline is configured using a `configuration.toml` file. Here's an example:

```toml
# Aggregation settings
[aggregation]
frequency_minutes = 15  # Group files in 15-minute intervals
check_interval_seconds = 300  # Check for new files every 5 minutes

# Provider configurations
[[providers]]
name = "ovapi"
timezone = "Europe/Amsterdam"

  [[providers.apis]]
  url = "https://gtfs.ovapi.nl/nl/vehiclePositions.pb"
  services = ["VehiclePosition"]
  refresh_seconds = 20  # Fetch every 20 seconds

  [[providers.apis]]
  url = "https://gtfs.ovapi.nl/nl/tripUpdates.pb"
  services = ["TripUpdate"]
  refresh_seconds = 30  # Fetch every 30 seconds
```

### Configuration Options

- **aggregation**: Settings for the aggregation process
  - **frequency_minutes**: The time interval (in minutes) for grouping files
  - **check_interval_seconds**: How often to check for new files to aggregate

- **providers**: List of GTFS-RT data providers
  - **name**: Name of the provider (used for directory structure)
  - **timezone**: Timezone for the provider's data
  - **apis**: List of API endpoints for this provider
    - **url**: URL of the GTFS-RT feed
    - **services**: List of service types to extract from the feed (VehiclePosition, TripUpdate, Alert,
      TripModifications)
    - **refresh_seconds**: How often to fetch data from this API

## Usage

Run the main script to start the pipeline:

```
python run_gtfs_rt_pipeline.py
```

This will start both the fetcher and aggregator processes. The fetcher will continuously fetch data from the configured
APIs, and the aggregator will group the files based on the configured time intervals.

## File Structure

The pipeline creates the following directory structure:

```
<provider_name>/
  ├── <service_type>/
  │   ├── individual_YYYY-MM-DD_HH-MM-SS.parquet  # Individual data files
  │   ├── processed_files.txt                     # List of processed files
  │   └── grouped/
  │       └── grouped_YYYY-MM-DD_HH-MM.parquet    # Grouped data files
  └── ...
```

## Scripts

- **run_gtfs_rt_pipeline.py**: Main script to run the entire pipeline
- **gtfs_rt_fetcher.py**: Script to fetch GTFS-RT data from providers
- **gtfs_rt_aggregator.py**: Script to aggregate individual files based on time intervals

## License

[MIT License](LICENSE) 