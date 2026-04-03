from typing import TypedDict

# ----------------------- #


class SQSQueueConfig(TypedDict, total=False):
    """Configuration for an SQS queue."""

    namespace: str
    """Base namespace for queues."""

    tenant_aware: bool
    """Whether the queue is tenant-aware."""
