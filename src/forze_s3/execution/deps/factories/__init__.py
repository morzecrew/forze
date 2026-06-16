"""S3 dependency factories."""

from .storage import (
    ConfigurableS3StorageCommand,
    ConfigurableS3StorageQuery,
    ConfigurableS3StorageUploads,
)

# ----------------------- #

__all__ = [
    "ConfigurableS3StorageQuery",
    "ConfigurableS3StorageCommand",
    "ConfigurableS3StorageUploads",
]
