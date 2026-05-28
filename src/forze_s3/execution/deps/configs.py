"""S3 storage execution configs."""

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class S3StorageConfig(TenantAwareIntegrationConfig):
    """Configuration for the S3 storage."""

    bucket: str
    """The name of the bucket to use for the storage."""
