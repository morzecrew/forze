"""S3 lifecycle steps (client pool startup and shutdown)."""

from .pool import (
    S3_CLIENT_CAPABILITY,
    S3BucketStartupHook,
    S3ShutdownHook,
    S3StartupHook,
    s3_bucket_lifecycle_step,
    s3_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "S3_CLIENT_CAPABILITY",
    "S3BucketStartupHook",
    "S3ShutdownHook",
    "S3StartupHook",
    "s3_bucket_lifecycle_step",
    "s3_lifecycle_step",
]
