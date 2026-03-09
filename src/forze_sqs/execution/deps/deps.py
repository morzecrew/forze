"""Factory functions for SQS queue adapters."""

from typing import Any

from forze.application.contracts.queue import (
    QueueConformity,
    QueueDepConformity,
    QueueSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to

from ...adapters import SQSQueueAdapter, SQSQueueCodec
from .keys import SQSClientDepKey

# ----------------------- #


@conforms_to(QueueDepConformity)
def sqs_queue(
    context: ExecutionContext,
    spec: QueueSpec[Any],
) -> QueueConformity:
    """Build an SQS-backed queue port for the given spec."""
    sqs_client = context.dep(SQSClientDepKey)
    codec = SQSQueueCodec(model=spec.model)

    return SQSQueueAdapter(
        client=sqs_client,
        codec=codec,
        namespace=spec.namespace,
    )
