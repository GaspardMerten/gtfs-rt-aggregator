import fnmatch
import os
from typing import List, Optional

from google.cloud import storage

from ..storage.base import StorageInterface


class GoogleCloudStorage(StorageInterface):
    """Google Cloud Storage implementation of the storage interface."""

    def __init__(self, bucket_name: str, base_path: str = ""):
        """
        Initialize Google Cloud Storage.

        Args:
            bucket_name: Name of the GCS bucket
            base_path: Base path within the bucket
        """
        self.bucket_name = bucket_name
        self.base_path = base_path.rstrip("/")
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def save_bytes(self, data: bytes, path: str) -> str:
        """Save binary data to Google Cloud Storage."""
        # Get full path
        blob_path = self._get_full_path(path)

        try:
            # In GCS, directories don't need to be explicitly created
            # Just upload the file directly
            blob = self.bucket.blob(blob_path)
            blob.upload_from_string(data)

            return f"gs://{self.bucket_name}/{blob_path}"
        except Exception as e:
            self.logger.error(f"Error saving data to GCS: {e}")
            return ""

    def read_bytes(self, path: str) -> bytes:
        """Read binary data from Google Cloud Storage."""
        try:
            # Extract blob path
            blob_path = self._extract_blob_path(path)

            # Get the blob
            blob = self.bucket.blob(blob_path)

            # Download as bytes
            return blob.download_as_bytes()
        except Exception as e:
            self.logger.error(f"Error reading file from GCS {path}: {e}")
            return b""

    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """List files in Google Cloud Storage matching a pattern."""
        # Get full directory path
        full_dir = self._get_full_path(directory)

        # List blobs with the directory prefix
        blobs = self.client.list_blobs(self.bucket_name, prefix=full_dir)

        # Filter blobs
        matching_blobs = []
        for blob in blobs:
            # Skip directories (objects ending with /)
            if blob.name.endswith("/"):
                continue

            # Apply pattern filter if provided
            if pattern and not fnmatch.fnmatch(os.path.basename(blob.name), pattern):
                continue

            # Add to results, removing the base path prefix
            relative_path = blob.name
            if self.base_path and blob.name.startswith(self.base_path + "/"):
                relative_path = blob.name[len(self.base_path) + 1 :]

            matching_blobs.append(relative_path)

        return matching_blobs

    def delete_file(self, path: str) -> bool:
        """Delete a file from Google Cloud Storage."""
        try:
            # Extract blob path
            blob_path = self._extract_blob_path(path)

            # Delete the blob
            blob = self.bucket.blob(blob_path)
            blob.delete()
            return True
        except Exception as e:
            self.logger.error(f"Error deleting file from GCS {path}: {e}")
            return False

    def rename_file(self, source_path: str, target_path: str) -> bool:
        """Rename or move a file in Google Cloud Storage."""
        try:
            # Extract blob paths
            source_blob_path = self._extract_blob_path(source_path)
            target_blob_path = self._extract_blob_path(target_path)

            # Get source blob
            source_blob = self.bucket.blob(source_blob_path)

            # Copy to target
            self.bucket.copy_blob(source_blob, self.bucket, target_blob_path)

            # Delete source
            source_blob.delete()

            return True
        except Exception as e:
            self.logger.error(
                f"Error renaming file in GCS from {source_path} to {target_path}: {e}"
            )
            return False

    def file_exists(self, path: str) -> bool:
        """Check if a file exists in Google Cloud Storage."""
        try:
            # Extract blob path
            blob_path = self._extract_blob_path(path)

            # Check if blob exists
            blob = self.bucket.blob(blob_path)
            return blob.exists()
        except Exception as e:
            self.logger.error(f"Error checking if file exists in GCS {path}: {e}")
            return False

    def _get_full_path(self, path: str) -> str:
        """
        Get the full path for a file or directory in Google Cloud Storage.

        Args:
            path: Relative path

        Returns:
            Full path in the bucket
        """
        if self.base_path:
            return f"{self.base_path}/{path}"
        return path

    def _extract_blob_path(self, path: str) -> str:
        """
        Extract blob path from a path or gs:// URL.

        Args:
            path: Path or gs:// URL

        Returns:
            Blob path
        """
        if path.startswith("gs://"):
            parts = path.replace("gs://", "").split("/", 1)
            if len(parts) != 2 or parts[0] != self.bucket_name:
                raise ValueError(f"Invalid GCS path: {path}")
            return parts[1]

        return self._get_full_path(path)
