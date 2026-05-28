"""GCS storage execution configs."""

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class GCSStorageConfig(TenantAwareIntegrationConfig):
    """Configuration for a GCS-backed storage route."""

    bucket: str
    """GCS bucket name."""
