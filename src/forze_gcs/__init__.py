"""Google Cloud Storage integration for Forze."""

from ._compat import require_gcs

require_gcs()

# ....................... #

from .execution import (
    GCSClientDepKey,
    GCSDepsModule,
    GCSStorageConfig,
    gcs_lifecycle_step,
    routed_gcs_lifecycle_step,
)
from .kernel.platform import (
    GCSClient,
    GCSClientPort,
    GCSConfig,
    GCSRoutingCredentials,
    RoutedGCSClient,
)

# ----------------------- #

__all__ = [
    "GCSDepsModule",
    "GCSClient",
    "GCSClientPort",
    "RoutedGCSClient",
    "GCSRoutingCredentials",
    "GCSConfig",
    "GCSClientDepKey",
    "gcs_lifecycle_step",
    "routed_gcs_lifecycle_step",
    "GCSStorageConfig",
]
