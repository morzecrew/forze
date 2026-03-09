"""SQS execution wiring for the application kernel."""

from .deps import SQSClientDepKey, SQSDepsModule
from .lifecycle import sqs_lifecycle_step

# ----------------------- #

__all__ = [
    "SQSDepsModule",
    "SQSClientDepKey",
    "sqs_lifecycle_step",
]
