"""RabbitMQ queue execution configs."""

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueConfig(TenantAwareIntegrationConfig):
    """Configuration for a RabbitMQ queue."""

    namespace: NamedResourceSpec = attrs.field(
        default="",
        converter=coerce_named_resource_spec,
    )
    """Base namespace for queues."""

    delayed_delivery: bool = False
    """When True, enable DLX delay-queue publishing for delayed enqueues."""
