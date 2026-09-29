import argparse
import logging

from ..config.loader import load_config_from_toml
from ..pipeline import run_pipeline_from_toml, scrub_static_urls
from ..utils.redact import install_redaction


def main():
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Run GTFS-RT pipeline from a TOML configuration file."
    )
    parser.add_argument(
        "toml_path", type=str, help="Path to the TOML configuration file"
    )

    # Add optional arguments
    parser.add_argument(
        "--log-level", type=str, default="INFO", help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--iceberg-backfill",
        action="store_true",
        help="Convert files stored before 0.6.0, then register every compacted day in Iceberg, then exit",
    )
    parser.add_argument(
        "--convert-old-files",
        action="store_true",
        help="Rewrite aggregated files stored before 0.6.0 with the current types, then exit",
    )
    parser.add_argument(
        "--scrub-urls",
        action="store_true",
        help="Remove query strings (API keys) from URLs saved in static manifests by versions before 0.5.1, then exit",
    )

    # Parse arguments
    args = parser.parse_args()

    # Call the run_pipeline_from_toml function
    try:
        logging.basicConfig(level=args.log_level)
        install_redaction()
        if args.iceberg_backfill:
            from ..aggregator.convert import convert_old_files
            from ..pipeline import create_storages
            from ..sinks.iceberg import IcebergSink

            config = load_config_from_toml(args.toml_path)
            storages = create_storages(config)
            converted = convert_old_files(config, storages)
            registered = IcebergSink(config, storages).sync(days_back=None)
            print(f"Converted {converted} files, registered {registered} days")
            return
        if args.convert_old_files:
            from ..aggregator.convert import convert_old_files
            from ..pipeline import create_storages

            config = load_config_from_toml(args.toml_path)
            count = convert_old_files(config, create_storages(config))
            print(f"Converted {count} files")
            return
        if args.scrub_urls:
            count = scrub_static_urls(load_config_from_toml(args.toml_path))
            print(f"Rewrote {count} manifest files")
            return
        run_pipeline_from_toml(args.toml_path)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
