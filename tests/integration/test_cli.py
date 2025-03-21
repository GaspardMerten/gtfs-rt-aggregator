"""Test the CLI functionality for the GTFS-RT aggregator."""

import os
import sys
import unittest
from io import StringIO
from unittest.mock import patch, MagicMock

from src.gtfs_rt_aggregator.utils.cli import main
from tests.mocks import MockServerManager


class TestCli(unittest.TestCase):
    """Test the CLI functionality."""

    @classmethod
    def setUpClass(cls):
        """Start the mock server before tests."""
        cls.server_manager = MockServerManager()
        success = cls.server_manager.start()
        if not success:
            raise RuntimeError("Could not start mock server")
        # Get the actual port that was used
        cls.mock_server_port = cls.server_manager.port

        # Update the test config to use the actual port
        cls._update_test_config_port(cls.mock_server_port)

    @classmethod
    def tearDownClass(cls):
        """Stop the mock server after tests."""
        cls.server_manager.stop()

        # Restore the original test config
        cls._restore_test_config()

    @staticmethod
    def _update_test_config_port(port):
        """Update the test config file to use the actual port."""
        config_path = os.path.join("data", "test_config.toml")
        with open(config_path, "r") as f:
            content = f.read()

        # Replace the port in the URLs
        updated_content = content.replace("8788", str(port))

        # Save the original content
        with open(config_path + ".original", "w") as f:
            f.write(content)

        # Write the updated content
        with open(config_path, "w") as f:
            f.write(updated_content)

    @staticmethod
    def _restore_test_config():
        """Restore the original test config file."""
        config_path = os.path.join("data", "test_config.toml")
        original_path = config_path + ".original"

        if os.path.exists(original_path):
            with open(original_path, "r") as f:
                original_content = f.read()

            with open(config_path, "w") as f:
                f.write(original_content)

            os.remove(original_path)

    @patch("gtfs_rt_aggregator.pipeline.run_pipeline")
    def test_cli_with_valid_config(self, mock_run_pipeline):
        """Test the CLI with a valid configuration file."""
        # Mock sys.argv
        test_args = [
            "gtfs_rt_aggregator",
            "data/test_config.toml",
            "--log-level",
            "DEBUG",
        ]
        with patch.object(sys, "argv", test_args):
            # Capture stdout
            captured_output = StringIO()
            with patch("sys.stdout", new=captured_output):
                # Run the CLI
                main()

        # Check that run_pipeline was called
        mock_run_pipeline.assert_called_once()

        # The config should have been loaded correctly
        config = mock_run_pipeline.call_args[0][0]
        self.assertEqual(config.providers[0].name, "test_provider")
        self.assertEqual(len(config.providers[0].apis), 3)

    @patch("sys.stderr", new_callable=StringIO)
    def test_cli_with_missing_args(self, mock_stderr):
        """Test the CLI with missing arguments."""
        # Mock sys.argv with missing required argument
        test_args = ["gtfs_rt_aggregator"]
        with patch.object(sys, "argv", test_args):
            # Run the CLI and expect SystemExit
            with self.assertRaises(SystemExit):
                main()

        # Check that the error message was output
        self.assertIn(
            "error: the following arguments are required: toml_path",
            mock_stderr.getvalue(),
        )

    @patch("builtins.print")
    def test_cli_with_invalid_config(self, mock_print):
        """Test the CLI with an invalid configuration file."""
        # Mock sys.argv with a non-existent config file
        test_args = ["gtfs_rt_aggregator", "nonexistent_file.toml"]
        with patch.object(sys, "argv", test_args):
            # Run the CLI
            main()

        # Check that an error message was printed
        # We don't care about the number of calls, just that the last one is the error
        self.assertTrue(mock_print.called)
        error_message = mock_print.call_args_list[-1][0][0]
        self.assertTrue(
            error_message.startswith("Error:"),
            f"Expected error message, got: {error_message}",
        )

    @patch("gtfs_rt_aggregator.pipeline.GtfsRtPipeline")
    def test_cli_pipeline_creation(self, mock_pipeline_class):
        """Test that the CLI creates and starts the pipeline."""
        # Create a mock pipeline instance
        mock_pipeline = MagicMock()
        mock_pipeline_class.return_value = mock_pipeline

        # Mock sys.argv
        test_args = ["gtfs_rt_aggregator", "data/test_config.toml"]
        with patch.object(sys, "argv", test_args):
            # Run the CLI
            main()

        # Check that the pipeline was created and started
        mock_pipeline_class.assert_called_once()
        mock_pipeline.start.assert_called_once()


if __name__ == "__main__":
    unittest.main()
