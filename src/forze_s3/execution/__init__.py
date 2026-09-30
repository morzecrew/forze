"""S3 execution wiring for the application kernel."""

from .deps import (
    S3ClientDepKey,
    S3DepsModule,
    S3ServerSideEncryption,
    S3StorageConfig,
)
from .lifecycle import S3_CLIENT_CAPABILITY, s3_bucket_lifecycle_step, s3_lifecycle_step

# ----------------------- #

__all__ = [
    "S3DepsModule",
    "S3ClientDepKey",
    "s3_lifecycle_step",
    "s3_bucket_lifecycle_step",
    "S3_CLIENT_CAPABILITY",
    "S3StorageConfig",
    "S3ServerSideEncryption",
]
