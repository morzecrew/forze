"""Factory functions for RabbitMQ queue adapters."""

from typing import Any

from forze.application.contracts.queue import (
    QueueConformity,
    QueueDepConformity,
    QueueSpec,
)
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to

from ...adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from .keys import RabbitMQClientDepKey

# ----------------------- #


@conforms_to(QueueDepConformity)
def rabbitmq_queue(
    context: ExecutionContext,
    spec: QueueSpec[Any],
) -> QueueConformity:
    """Build a RabbitMQ-backed queue port for the given spec."""
    rabbitmq_client = context.dep(RabbitMQClientDepKey)
    codec = RabbitMQQueueCodec(model=spec.model)

    return RabbitMQQueueAdapter(
        client=rabbitmq_client,
        codec=codec,
        namespace=spec.namespace,
    )
