"""Temporal workflow execution configs."""

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalWorkflowConfig(TenantAwareIntegrationConfig):
    """Configuration for a Temporal workflow."""

    queue: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """Temporal task queue name."""
