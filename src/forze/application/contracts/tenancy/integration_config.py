"""Integration wiring configs shared across storage/search adapters."""

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TenantAwareIntegrationConfig:
    """Base for integration dependency configs that opt into tenant-scoped data access.

    Distinct from :class:`~forze.application.contracts.tenancy.mixins.TenancyMixin`, which
    carries runtime ``tenant_provider`` wiring on adapter instances.
    """

    tenant_aware: bool = False
    """When ``True``, adapters apply tenant filters using the execution context tenant."""
