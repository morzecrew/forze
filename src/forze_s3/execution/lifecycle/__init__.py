"""S3 lifecycle steps (client pool startup and shutdown)."""

from .pool import (
    S3ShutdownHook,
    S3StartupHook,
    s3_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "S3ShutdownHook",
    "S3StartupHook",
    "s3_lifecycle_step",
]
