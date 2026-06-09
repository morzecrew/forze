"""S3 dependency factories."""

from .storage import ConfigurableS3StorageCommand, ConfigurableS3StorageQuery

# ----------------------- #

__all__ = ["ConfigurableS3StorageQuery", "ConfigurableS3StorageCommand"]
