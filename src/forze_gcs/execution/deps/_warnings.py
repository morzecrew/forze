"""Private tenancy warning descriptors for GCS deps module."""

from forze.application.contracts.tenancy import IntegrationRouteWarning

from .configs import GCSStorageConfig

# ----------------------- #

GCS_STORAGE_WARNING = IntegrationRouteWarning[GCSStorageConfig](
    kind="storage",
    tenant_aware=lambda config: config.tenant_aware,
    named_fields=lambda config: [("bucket", config.bucket)],
)
