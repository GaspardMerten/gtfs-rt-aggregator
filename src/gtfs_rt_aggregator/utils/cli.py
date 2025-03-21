import argparse
import logging

from gtfs_rt_aggregator import run_pipeline_from_toml


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

    # Parse arguments
    args = parser.parse_args()

    # Call the run_pipeline_from_toml function
    try:
        logging.basicConfig(level=args.log_level)
        run_pipeline_from_toml(args.toml_path)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
