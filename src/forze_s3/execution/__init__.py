"""S3 execution wiring for the application kernel."""

from .deps import S3ClientDepKey, S3DepsModule, S3StorageConfig
from .lifecycle import routed_s3_lifecycle_step, s3_lifecycle_step

# ----------------------- #

__all__ = [
    "S3DepsModule",
    "S3ClientDepKey",
    "s3_lifecycle_step",
    "routed_s3_lifecycle_step",
    "S3StorageConfig",
]
