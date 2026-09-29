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
        if args.scrub_urls:
            count = scrub_static_urls(load_config_from_toml(args.toml_path))
            print(f"Rewrote {count} manifest files")
            return
        run_pipeline_from_toml(args.toml_path)
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
