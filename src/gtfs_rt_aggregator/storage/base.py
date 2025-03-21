from abc import ABC, abstractmethod
from typing import List, Optional

from ..utils.log_helper import setup_logger


class StorageInterface(ABC):
    """Abstract base class for storage implementations."""

    def __init__(self):
        """Initialize the storage interface with a logger."""
        self.logger = setup_logger(
            f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        self.logger.debug(f"Initializing {self.__class__.__name__}")

    @abstractmethod
    def save_bytes(self, data: bytes, path: str) -> str:
        """
        Save binary data to storage.

        @param data: Binary data to save
        @param path: Path where to save the data
        @return Path or identifier of the saved data
        """
        pass

    @abstractmethod
    def read_bytes(self, path: str) -> bytes:
        """
        Read binary data from storage.

        @param path: Path or identifier of the data to read
        @return Binary data
        """
        pass

    @abstractmethod
    def list_files(self, directory: str, pattern: Optional[str] = None) -> List[str]:
        """
        List files in storage matching a pattern.

        @param directory: Directory to list files from
        @param pattern: Pattern to match files against (optional)
        @return List of file paths or identifiers
        """
        pass

    @abstractmethod
    def delete_file(self, path: str) -> bool:
        """
        Delete a file from storage.

        @param path: Path or identifier of the file to delete
        @return True if the file was deleted, False otherwise
        """
        pass

    @abstractmethod
    def rename_file(self, source_path: str, target_path: str) -> bool:
        """
        Rename or move a file in storage.

        @param source_path: Path or identifier of the file to rename
        @param target_path: New path or identifier for the file
        @return True if the file was renamed, False otherwise
        """
        pass

    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """
        Check if a file exists in storage.

        @param path: Path or identifier of the file to check
        @return True if the file exists, False otherwise
        """
        pass
