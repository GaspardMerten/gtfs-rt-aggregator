import glob
import os
from pathlib import Path
from typing import List, Optional

from ..storage.base import StorageInterface


class FileSystemStorage(StorageInterface):
    """File system implementation of the storage interface."""

    def __init__(self, base_directory: str = "."):
        """
        Initialize file system storage.

        Args:
            base_directory: Base directory for storing data
        """
        super().__init__()
        self.base_directory = base_directory
        self.logger.info(
            f"Initialized file system storage with base directory: {base_directory}"
        )

    def save_bytes(self, data: bytes, path: str) -> str:
        """Save binary data to the file system."""
        self.logger.debug(f"Saving {len(data)} bytes to {path}")

        # Get full path
        full_path = self._get_full_path(path)

        # Ensure directory exists
        directory = os.path.dirname(full_path)
        if not self._ensure_directory(directory):
            self.logger.error(f"Failed to create directory: {directory}")
            raise IOError(f"Failed to create directory: {directory}")

        try:
            # Write the data
            with open(full_path, "wb") as f:
                f.write(data)

            self.logger.debug(f"Successfully saved data to {full_path}")
            return full_path
        except Exception as e:
            self.logger.error(
                f"Error saving data to {full_path}: {str(e)}", exc_info=True
            )
            raise

    def read_bytes(self, path: str) -> bytes:
        """Read binary data from the file system."""
        self.logger.debug(f"Reading data from {path}")

        try:
            full_path = self._get_full_path(path)
            with open(full_path, "rb") as f:
                data = f.read()

            self.logger.debug(f"Successfully read {len(data)} bytes from {full_path}")
            return data
        except Exception as e:
            self.logger.error(f"Error reading file {path}: {str(e)}", exc_info=True)
            return b""

    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """List files in the file system matching a pattern."""
        self.logger.debug(
            f"Listing files in {directory}"
            + (f" with pattern {pattern}" if pattern else "")
        )

        full_dir = self._get_full_path(directory)

        try:
            if pattern:
                search_pattern = os.path.join(full_dir, pattern)
                files = glob.glob(search_pattern)
                self.logger.debug(
                    f"Found {len(files)} files matching pattern {pattern} in {full_dir}"
                )
            else:
                try:
                    files = [
                        os.path.join(full_dir, f)
                        for f in os.listdir(full_dir)
                        if os.path.isfile(os.path.join(full_dir, f))
                    ]
                    self.logger.debug(f"Found {len(files)} files in {full_dir}")
                except FileNotFoundError:
                    self.logger.warning(f"Directory not found: {full_dir}")
                    files = []

            # Convert to relative paths
            base_path = Path(self.base_directory).resolve()
            relative_paths = [
                str(Path(f).resolve().relative_to(base_path)) for f in files
            ]

            return relative_paths
        except Exception as e:
            self.logger.error(
                f"Error listing files in {directory}: {str(e)}", exc_info=True
            )
            return []

    def delete_file(self, path: str) -> bool:
        """Delete a file from the file system."""
        self.logger.debug(f"Deleting file {path}")

        try:
            full_path = self._get_full_path(path)
            if os.path.exists(full_path):
                os.remove(full_path)
                self.logger.debug(f"Successfully deleted file {full_path}")
                return True
            else:
                self.logger.warning(f"File not found for deletion: {full_path}")
                return False
        except Exception as e:
            self.logger.error(f"Error deleting file {path}: {str(e)}", exc_info=True)
            return False

    def rename_file(self, source_path: str, target_path: str) -> bool:
        """Rename or move a file in the file system."""
        self.logger.debug(f"Renaming file from {source_path} to {target_path}")

        try:
            source_full_path = self._get_full_path(source_path)
            target_full_path = self._get_full_path(target_path)

            # Ensure target directory exists
            target_dir = os.path.dirname(target_full_path)
            if not self._ensure_directory(target_dir):
                self.logger.error(f"Failed to create target directory: {target_dir}")
                return False

            # Rename the file
            os.rename(source_full_path, target_full_path)
            self.logger.debug(
                f"Successfully renamed file from {source_full_path} to {target_full_path}"
            )
            return True
        except Exception as e:
            self.logger.error(
                f"Error renaming file {source_path} to {target_path}: {str(e)}",
                exc_info=True,
            )
            return False

    def file_exists(self, path: str) -> bool:
        """Check if a file exists in the file system."""
        full_path = self._get_full_path(path)
        exists = os.path.isfile(full_path)
        self.logger.debug(f"Checking if file exists: {path} -> {exists}")
        return exists

    def _get_full_path(self, path: str) -> str:
        """
        Get the full path for a file or directory in the file system.

        Args:
            path: Relative path

        Returns:
            Full path
        """
        return os.path.join(self.base_directory, path)

    def _ensure_directory(self, directory_path: str) -> bool:
        """
        Ensure a directory exists in the file system.

        Args:
            directory_path: Path of the directory to ensure

        Returns:
            True if directory exists or was created, False otherwise
        """
        try:
            self.logger.debug(f"Ensuring directory exists: {directory_path}")
            os.makedirs(directory_path, exist_ok=True)
            return True
        except Exception as e:
            self.logger.error(
                f"Error creating directory {directory_path}: {str(e)}", exc_info=True
            )
            return False
