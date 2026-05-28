"""Temporal workflow execution configs."""

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalWorkflowConfig(TenantAwareIntegrationConfig):
    """Configuration for a Temporal workflow."""

    queue: str
    """Temporal task queue name."""
