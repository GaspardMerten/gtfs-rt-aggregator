import io
import os
from typing import List, Optional

from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

from ..storage.base import StorageInterface


class MinioStorage(StorageInterface):
    """MinIO (S3-compatible) implementation of the storage interface."""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket_name: str,
        secure: bool = True,
        base_path: str = "",
    ):
        """
        Initialize MinIO storage.

        Args:
            endpoint: MinIO server endpoint without scheme (e.g., 'minio.example.com:9000')
            access_key: MinIO access key
            secret_key: MinIO secret key
            bucket_name: Name of the bucket to use
            secure: Whether to use HTTPS or HTTP
            base_path: Base path in the bucket
        """
        super().__init__()
        self.endpoint = endpoint
        self.bucket_name = bucket_name
        self.base_path = base_path.strip("/")

        # Initialize MinIO client
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

        # Ensure the bucket exists
        if not self.client.bucket_exists(bucket_name):
            self.logger.info(f"Bucket '{bucket_name}' does not exist, creating it")
            self.client.make_bucket(bucket_name)

        self.logger.info(
            f"Initialized MinIO storage with endpoint: {endpoint}, bucket: {bucket_name}"
        )

    def save_bytes(self, data: bytes, path: str) -> str:
        """Save binary data to MinIO storage."""
        self.logger.debug(f"Saving {len(data)} bytes to {path}")

        object_name = self._get_object_name(path)

        try:
            # Write the data
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=io.BytesIO(data),
                length=len(data),
            )

            self.logger.debug(f"Successfully saved data to {object_name}")
            return path
        except S3Error as e:
            self.logger.error(
                f"Error saving data to {object_name}: {str(e)}", exc_info=True
            )
            raise

    def read_bytes(self, path: str) -> bytes:
        """Read binary data from MinIO storage."""
        self.logger.debug(f"Reading data from {path}")

        object_name = self._get_object_name(path)

        try:
            # Get the object
            response = self.client.get_object(
                bucket_name=self.bucket_name, object_name=object_name
            )

            # Read the data
            data = response.read()
            response.close()

            self.logger.debug(f"Successfully read {len(data)} bytes from {object_name}")
            return data
        except S3Error as e:
            self.logger.error(
                f"Error reading data from {object_name}: {str(e)}", exc_info=True
            )
            raise

    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """List files in MinIO storage matching a pattern."""
        self.logger.debug(f"Listing files in {directory} (pattern: {pattern})")

        prefix = self._get_object_name(directory)
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        results = []

        try:
            # List objects in the bucket with the given prefix
            objects = self.client.list_objects(
                bucket_name=self.bucket_name, prefix=prefix, recursive=True
            )

            # Process the objects
            for obj in objects:
                object_name = obj.object_name

                # Remove the base_path prefix to get the relative path
                if self.base_path:
                    relative_path = object_name[len(self.base_path) :].lstrip("/")
                else:
                    relative_path = object_name

                # If a pattern is specified, check if the file matches
                if pattern:
                    import fnmatch

                    if not fnmatch.fnmatch(os.path.basename(relative_path), pattern):
                        continue

                results.append(relative_path)

            self.logger.debug(f"Found {len(results)} files in {directory}")
            return results
        except S3Error as e:
            self.logger.error(
                f"Error listing files in {directory}: {str(e)}", exc_info=True
            )
            raise

    def delete_file(self, path: str) -> bool:
        """Delete a file from MinIO storage."""
        self.logger.debug(f"Deleting file {path}")

        object_name = self._get_object_name(path)

        try:
            # Remove the object
            self.client.remove_object(
                bucket_name=self.bucket_name, object_name=object_name
            )

            self.logger.debug(f"Successfully deleted {object_name}")
            return True
        except S3Error as e:
            self.logger.error(f"Error deleting {object_name}: {str(e)}", exc_info=True)
            return False

    def rename_file(self, source_path: str, target_path: str) -> bool:
        """Rename or move a file in MinIO storage."""
        self.logger.debug(f"Renaming file from {source_path} to {target_path}")

        source_object = self._get_object_name(source_path)
        target_object = self._get_object_name(target_path)

        try:
            # Copy object to the new location
            self.client.copy_object(
                bucket_name=self.bucket_name,
                object_name=target_object,
                source=CopySource(self.bucket_name, source_object),
            )

            # Delete the source object
            self.client.remove_object(
                bucket_name=self.bucket_name, object_name=source_object
            )

            self.logger.debug(
                f"Successfully renamed {source_object} to {target_object}"
            )
            return True
        except S3Error as e:
            self.logger.error(
                f"Error renaming {source_object} to {target_object}: {str(e)}",
                exc_info=True,
            )
            return False

    def file_exists(self, path: str) -> bool:
        """Check if a file exists in MinIO storage."""
        self.logger.debug(f"Checking if file {path} exists")

        object_name = self._get_object_name(path)

        try:
            # Try to get the object stats
            self.client.stat_object(
                bucket_name=self.bucket_name, object_name=object_name
            )

            self.logger.debug(f"File {object_name} exists")
            return True
        except S3Error:
            self.logger.debug(f"File {object_name} does not exist")
            return False

    def _get_object_name(self, path: str) -> str:
        """Get the full object name for a path."""
        path = path.strip("/")
        if self.base_path:
            return f"{self.base_path}/{path}" if path else self.base_path
        return path
