from gtfs_rt_aggregator.storage.base import StorageInterface
from gtfs_rt_aggregator.storage.factory import (
    StorageFactory,
    create_storage_from_config,
)
from gtfs_rt_aggregator.storage.filesystem import FileSystemStorage
from gtfs_rt_aggregator.storage.gcs import GoogleCloudStorage
from gtfs_rt_aggregator.storage.minio import MinioStorage
from gtfs_rt_aggregator.utils.log_helper import setup_logger

__all__ = [
    "StorageInterface",
    "FileSystemStorage",
    "GoogleCloudStorage",
    "MinioStorage",
    "StorageFactory",
    "create_storage",
    "create_storage_from_config",
]

logger = setup_logger(__name__)


def create_storage(storage_type: str, **kwargs) -> StorageInterface:
    """
    Factory function to create a storage implementation.

    @param storage_type: Type of storage to create ('filesystem', 'gcs', or 'minio')
    @param kwargs: Additional arguments to pass to the storage constructor
    @return An instance of a StorageInterface implementation
    @raises ValueError: If the storage type is not supported
    """
    logger.debug(f"Creating storage of type: {storage_type}")

    try:
        if storage_type.lower() == "filesystem":
            base_directory = kwargs.get("base_directory", "")
            logger.debug(
                f"Creating FileSystemStorage with base_directory: {base_directory}"
            )
            return FileSystemStorage(base_directory=base_directory)
        elif storage_type.lower() in ("gcs", "google", "google_cloud_storage"):
            bucket_name = kwargs.get("bucket_name")
            if not bucket_name:
                logger.error("bucket_name is required for Google Cloud Storage")
                raise ValueError("bucket_name is required for Google Cloud Storage")
            base_path = kwargs.get("base_path", "")
            logger.debug(
                f"Creating GoogleCloudStorage with bucket: {bucket_name}, base_path: {base_path}"
            )
            return GoogleCloudStorage(bucket_name=bucket_name, base_path=base_path)
        elif storage_type.lower() in ("minio", "s3"):
            endpoint = kwargs.get("endpoint")
            access_key = kwargs.get("access_key")
            secret_key = kwargs.get("secret_key")
            bucket_name = kwargs.get("bucket_name")

            if not all([endpoint, access_key, secret_key, bucket_name]):
                missing = []
                if not endpoint:
                    missing.append("endpoint")
                if not access_key:
                    missing.append("access_key")
                if not secret_key:
                    missing.append("secret_key")
                if not bucket_name:
                    missing.append("bucket_name")
                error_msg = f"Missing required parameters for MinIO storage: {', '.join(missing)}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            secure = kwargs.get("secure", True)
            base_path = kwargs.get("base_path", "")
            logger.debug(
                f"Creating MinioStorage with endpoint: {endpoint}, bucket: {bucket_name}"
            )
            return MinioStorage(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                bucket_name=bucket_name,
                secure=secure,
                base_path=base_path,
            )
        else:
            logger.error(f"Unsupported storage type: {storage_type}")
            raise ValueError(f"Unsupported storage type: {storage_type}")
    except Exception as e:
        logger.error(
            f"Error creating storage of type {storage_type}: {str(e)}", exc_info=True
        )
        raise
