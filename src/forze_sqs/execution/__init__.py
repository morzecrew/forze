"""SQS execution wiring for the application kernel."""

from .deps import SQSClientDepKey, SQSDepsModule, SQSQueueConfig
from .lifecycle import routed_sqs_lifecycle_step, sqs_lifecycle_step

# ----------------------- #

__all__ = [
    "SQSDepsModule",
    "SQSClientDepKey",
    "sqs_lifecycle_step",
    "routed_sqs_lifecycle_step",
    "SQSQueueConfig",
]
