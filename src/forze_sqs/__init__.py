"""SQS integration for Forze.

Supports Amazon SQS-compatible queue services such as
Yandex Message Queue, LocalStack SQS, and Amazon SQS.
"""

from ._compat import require_sqs

require_sqs()

# ....................... #

from .execution import (
    SQSClientDepKey,
    SQSDepsModule,
    SQSQueueConfig,
    sqs_lifecycle_step,
)
from .kernel.platform import SQSClient, SQSConfig

# ----------------------- #

__all__ = [
    "SQSClient",
    "SQSConfig",
    "SQSClientDepKey",
    "SQSDepsModule",
    "sqs_lifecycle_step",
    "SQSQueueConfig",
]
