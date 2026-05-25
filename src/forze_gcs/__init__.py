"""Google Cloud Storage integration for Forze."""

from ._compat import require_gcs

require_gcs()

# ....................... #

from .execution import (
    GCSClientDepKey,
    GCSDepsModule,
    GCSStorageConfig,
    gcs_lifecycle_step,
)
from .kernel.platform import GCSClient, GCSClientPort, GCSConfig

# ----------------------- #

__all__ = [
    "GCSDepsModule",
    "GCSClient",
    "GCSClientPort",
    "GCSConfig",
    "GCSClientDepKey",
    "gcs_lifecycle_step",
    "GCSStorageConfig",
]
