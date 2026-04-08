"""S3 execution wiring for the application kernel."""

from .deps import S3ClientDepKey, S3DepsModule, S3StorageConfig
from .lifecycle import s3_lifecycle_step

# ----------------------- #

__all__ = ["S3DepsModule", "S3ClientDepKey", "s3_lifecycle_step", "S3StorageConfig"]
