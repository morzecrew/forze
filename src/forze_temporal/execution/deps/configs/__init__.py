"""Temporal workflow execution configs."""

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

from ....kernel.client import TemporalStartOptions

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TemporalWorkflowConfig(TenantAwareIntegrationConfig):
    """Configuration for a Temporal workflow."""

    queue: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """Temporal task queue name."""

    start_options: TemporalStartOptions | None = None
    """Retry policy, timeouts and id-reuse policy for starts of this workflow kind.

    Applies to starts made through the durable workflow command port. A **schedule** for
    the same workflow carries its own action and does not pick these up — the schedule's
    overlap policy governs there, and two of these fields (``id_reuse_policy``,
    ``start_delay``) have no meaning for a scheduled action at all.
    """
