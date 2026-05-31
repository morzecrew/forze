"""SQS queue execution configs."""

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueConfig(TenantAwareIntegrationConfig):
    """Configuration for an SQS queue."""

    namespace: NamedResourceSpec = attrs.field(
        default="",
        converter=coerce_named_resource_spec,
    )
    """Base namespace for queues."""
