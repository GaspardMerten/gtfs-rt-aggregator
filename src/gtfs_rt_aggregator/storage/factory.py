from typing import Dict, Type

from ..config.models import StorageConfig
from ..storage.base import StorageInterface
from ..storage.filesystem import FileSystemStorage
from ..storage.gcs import GoogleCloudStorage
from ..storage.minio import MinioStorage
from ..utils.log_helper import setup_logger

logger = setup_logger(__name__)


class StorageFactory:
    """Factory for creating storage implementations."""

    # Registry of storage implementations
    _registry: Dict[str, Type[StorageInterface]] = {
        "filesystem": FileSystemStorage,
        "gcs": GoogleCloudStorage,
        "google": GoogleCloudStorage,
        "google_cloud_storage": GoogleCloudStorage,
        "minio": MinioStorage,
        "s3": MinioStorage,
    }

    @classmethod
    def register_storage_type(
        cls, type_name: str, storage_class: Type[StorageInterface]
    ) -> None:
        """
        Register a new storage implementation.

        @param type_name: Name of the storage type
        @param storage_class: Storage implementation class
        """
        logger.debug(
            f"Registering storage type: {type_name} -> {storage_class.__name__}"
        )
        cls._registry[type_name.lower()] = storage_class

    @classmethod
    def create_from_config(cls, config: StorageConfig) -> StorageInterface:
        """
        Create a storage implementation from a configuration.

        @param config: Storage configuration
        @return Storage implementation
        @raises ValueError: If the storage type is not supported
        """
        storage_type = config.type.lower()
        logger.debug(f"Creating storage from config, type: {storage_type}")

        if storage_type not in cls._registry:
            logger.error(f"Unsupported storage type: {storage_type}")
            raise ValueError(f"Unsupported storage type: {storage_type}")

        storage_class = cls._registry[storage_type]
        logger.debug(f"Using storage class: {storage_class.__name__}")

        try:
            # noinspection PyArgumentList
            storage = storage_class(**config.params)
            logger.info(f"Created {storage_class.__name__} storage")
            return storage
        except Exception as e:
            logger.error(
                f"Error creating storage of type {storage_type}: {str(e)}",
                exc_info=True,
            )
            raise


def create_storage_from_config(config: StorageConfig) -> StorageInterface:
    """
    Create a storage implementation from a configuration.

    @param config: Storage configuration
    @return Storage implementation
    @raises ValueError: If the storage type is not supported
    """
    logger.debug(f"Creating storage from config: {config.type}")
    return StorageFactory.create_from_config(config)
