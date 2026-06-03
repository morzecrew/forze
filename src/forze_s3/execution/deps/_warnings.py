"""Private tenancy warning descriptors for S3 deps module."""

from forze.application.contracts.tenancy import IntegrationRouteWarning

from .configs import S3StorageConfig

# ----------------------- #

S3_STORAGE_WARNING = IntegrationRouteWarning[S3StorageConfig](
    kind="storage",
    tenant_aware=lambda config: config.tenant_aware,
    named_fields=lambda config: [("bucket", config.bucket)],
)
