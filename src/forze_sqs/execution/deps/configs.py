"""SQS queue execution configs."""

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SQSQueueConfig(TenantAwareIntegrationConfig):
    """Configuration for an SQS queue."""

    namespace: str = ""
    """Base namespace for queues."""
