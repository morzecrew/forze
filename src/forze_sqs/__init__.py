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
    routed_sqs_lifecycle_step,
    sqs_lifecycle_step,
)
from .kernel.platform import (
    RoutedSQSClient,
    SQSClient,
    SQSClientPort,
    SQSConfig,
    SQSQueueMessage,
    SQSRoutingCredentials,
)

# ----------------------- #

__all__ = [
    "SQSClient",
    "SQSClientPort",
    "SQSConfig",
    "RoutedSQSClient",
    "SQSRoutingCredentials",
    "SQSQueueMessage",
    "SQSDepsModule",
    "SQSClientDepKey",
    "sqs_lifecycle_step",
    "routed_sqs_lifecycle_step",
    "SQSQueueConfig",
]
