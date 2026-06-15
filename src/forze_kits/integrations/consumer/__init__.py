"""Queue-consumer runner: consume → park/dedupe/process → ack/nack, prewired."""

from .lifecycle import queue_consumer_background_lifecycle_step
from .runner import ConsumerRunResult, QueueConsumer

# ----------------------- #

__all__ = [
    "ConsumerRunResult",
    "QueueConsumer",
    "queue_consumer_background_lifecycle_step",
]
