"""RabbitMQ queue execution configs."""

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class RabbitMQQueueConfig(TenantAwareIntegrationConfig):
    """Configuration for a RabbitMQ queue."""

    namespace: str = ""
    """Base namespace for queues."""

    delayed_delivery: bool = False
    """When True, enable DLX delay-queue publishing for delayed enqueues."""
