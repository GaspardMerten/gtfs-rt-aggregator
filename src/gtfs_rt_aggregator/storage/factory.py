"""Deprecated since 0.7.4: use gtfs_rt_aggregator.storage.create_storage."""

import warnings

from ..config.models import StorageConfig
from ..storage.base import StorageInterface


def _deprecated():
    warnings.warn(
        "StorageFactory and create_storage_from_config are deprecated: use "
        "create_storage(config.type, **config.params)",
        DeprecationWarning,
        stacklevel=3,
    )


def create_storage_from_config(config: StorageConfig) -> StorageInterface:
    from . import create_storage

    _deprecated()
    return create_storage(config.type, **config.params)


class StorageFactory:
    @classmethod
    def create_from_config(cls, config: StorageConfig) -> StorageInterface:
        from . import create_storage

        _deprecated()
        return create_storage(config.type, **config.params)
