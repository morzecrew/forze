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
from .kernel.client import (
    GCSClient,
    GCSClientPort,
    GCSConfig,
    GCSRoutingCredentials,
    RoutedGCSClient,
)
from .kernel.relation import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
    resolve_gcs_bucket,
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
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "is_static_named_resource",
    "resolve_gcs_bucket",
]
