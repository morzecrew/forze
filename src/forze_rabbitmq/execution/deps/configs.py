from typing import TypedDict

# ----------------------- #


class RabbitMQQueueConfig(TypedDict, total=False):
    """Configuration for a RabbitMQ queue."""

    namespace: str
    """Base namespace for queues."""

    tenant_aware: bool
    """Whether the queue is tenant-aware."""

    delayed_delivery: bool
    """When ``True``, enable DLX delay-queue publishing for ``delay`` / ``not_before`` enqueues."""
