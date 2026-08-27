"""GCS execution wiring for the application kernel."""

from .deps import GCSClientDepKey, GCSDepsModule, GCSStorageConfig
from .lifecycle import gcs_lifecycle_step

# ----------------------- #

__all__ = [
    "GCSDepsModule",
    "GCSClientDepKey",
    "gcs_lifecycle_step",
    "GCSStorageConfig",
]
