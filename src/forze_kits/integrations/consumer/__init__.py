"""Queue-consumer runner: consume → park/dedupe/process → ack/nack, prewired."""

from .lifecycle import queue_consumer_background_lifecycle_step
from .runner import ConsumerRunResult, run_consumer

# ----------------------- #

__all__ = [
    "ConsumerRunResult",
    "queue_consumer_background_lifecycle_step",
    "run_consumer",
]
