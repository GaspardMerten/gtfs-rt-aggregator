import fnmatch
import http.server
import os
import re
import socketserver
import threading
import time
from pathlib import Path
from typing import List, Optional

from gtfs_rt_aggregator.storage.base import StorageInterface


class MockStorageInterface(StorageInterface):
    """Mock storage for testing."""

    def __init__(self):
        super().__init__()
        self.saved_data = {}

    def save_bytes(self, data: bytes, path: str) -> str:
        self.saved_data[path] = data
        return path

    def read_bytes(self, path: str) -> bytes:
        if path not in self.saved_data:
            raise FileNotFoundError(f"File not found: {path}")
        return self.saved_data.get(path)

    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        files = []
        for path in self.saved_data.keys():
            if path.startswith(directory):
                if pattern is None:
                    files.append(path)
                elif "*" in pattern:
                    # Handle glob patterns
                    if fnmatch.fnmatch(os.path.basename(path), pattern):
                        files.append(path)
                elif re.search(pattern, path):
                    # Handle regex patterns
                    files.append(path)
        return files

    def delete_file(self, path: str) -> bool:
        if path in self.saved_data:
            del self.saved_data[path]
            return True
        return False

    def rename_file(self, source_path: str, target_path: str) -> bool:
        if source_path in self.saved_data:
            self.saved_data[target_path] = self.saved_data[source_path]
            del self.saved_data[source_path]
            return True
        return False

    def file_exists(self, path: str) -> bool:
        return path in self.saved_data

    # Legacy methods to maintain backward compatibility with tests
    def get_bytes(self, path: str) -> bytes:
        return self.read_bytes(path)

    def list_paths(self, prefix: str = "") -> list:
        return self.list_files(prefix)

    def delete(self, path: str) -> bool:
        return self.delete_file(path)


class MockGtfsRtServer(http.server.SimpleHTTPRequestHandler):
    """Mock HTTP server that serves GTFS-RT data from test files."""

    # Map API endpoints to test data files
    ENDPOINT_MAPPING = {
        "/alerts": "alerts.pb",
        "/trip_updates": "trip_updates.pb",
        "/vehicle_positions": "vehicle_positions.pb",
    }

    def do_GET(self):
        # Get the file path for the requested endpoint
        test_file = self.ENDPOINT_MAPPING.get(self.path)

        if test_file:
            # Return the file's contents
            self.send_response(200)
            self.send_header("Content-type", "application/x-protobuf")

            test_file_path = str(Path(__file__).parent / "data" / test_file)

            try:
                with open(test_file_path, "rb") as f:
                    data = f.read()

                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except FileNotFoundError:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(f"Test file not found: {test_file}".encode("utf-8"))
        else:
            # Return 404 for unknown endpoints
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Not Found")

    def log_message(self, format, *args):
        """Suppress log messages to prevent cluttering test output."""
        pass


class MockServerManager:
    """Helper class to manage mock server instances for tests."""

    def __init__(self, port=None):
        self.port = port or int(os.environ.get("MOCKUP_SERVER_PORT", 8788))
        self.server = None
        self.server_thread = None

    def start(self, handler_class=MockGtfsRtServer):
        """Start the mock server on the specified port or find an available port."""
        # If no specific port is requested, try to find an available port
        if not self.server:
            # Try the specified port first
            success = self._try_start_server(self.port, handler_class)

            # If that fails, try to find another port
            if (
                not success and self.port == 8788
            ):  # Only search if using the default port
                for port in range(8789, 8889):
                    success = self._try_start_server(port, handler_class)
                    if success:
                        self.port = port
                        break

            return success
        return False

    def _try_start_server(self, port, handler_class):
        """Try to start the server on the specified port."""
        try:
            self.server = socketserver.TCPServer(("localhost", port), handler_class)
            self.server_thread = threading.Thread(target=self.server.serve_forever)
            self.server_thread.daemon = True
            self.server_thread.start()

            # Wait for server to start
            time.sleep(0.5)
            print(f"Mock server started on port {port}")
            return True
        except OSError:
            print(f"Failed to start mock server on port {port}")
            return False

    def stop(self):
        """Stop the mock server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.server_thread.join(1)
            print("Mock server stopped")


def create_mock_parquet_files(
    storage: MockStorageInterface,
    provider_name: str,
    service_type: str,
    count: int,
    base_time_str: str,
) -> List[str]:
    """
    Helper function to create mock parquet files in storage for testing aggregation.

    Args:
        storage: The mock storage interface
        provider_name: Provider name
        service_type: Service type (e.g., 'VehiclePosition')
        count: Number of files to create
        base_time_str: Base time string in ISO format (e.g., '2023-01-01T12:00:00')

    Returns:
        List of created file paths
    """
    # Import here to avoid circular imports
    from datetime import datetime, timedelta
    import pandas as pd
    from io import BytesIO

    # Parse base time
    base_time = datetime.fromisoformat(base_time_str)
    created_files = []

    for i in range(count):
        # Create timestamp for this file
        timestamp = base_time + timedelta(minutes=i * 5)
        timestamp_str = timestamp.strftime(
            "%Y-%m-%d_%H-%M-%S"
        )  # Use dash format instead

        # Create file path with the 'individual_' prefix
        file_path = f"{provider_name}/{service_type}/individual/individual_{timestamp_str}.parquet"

        # Create a simple DataFrame
        df = pd.DataFrame(
            {
                "id": [f"test_id_{i}_{j}" for j in range(5)],
                "fetch_time": [timestamp for _ in range(5)],
                "provider": [provider_name for _ in range(5)],
                "service_type": [service_type for _ in range(5)],
                "value": [j for j in range(5)],
            }
        )

        # Save to storage
        buffer = BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)
        storage.save_bytes(buffer.read(), file_path)
        created_files.append(file_path)

    return created_files
