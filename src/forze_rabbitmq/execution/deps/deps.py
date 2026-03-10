"""Factory functions for RabbitMQ queue adapters."""

from typing import Any

from forze.application.contracts.queue import QueueSpec
from forze.application.execution import ExecutionContext

from ...adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from .keys import RabbitMQClientDepKey

# ----------------------- #


def rabbitmq_queue(
    context: ExecutionContext,
    spec: QueueSpec[Any],
) -> RabbitMQQueueAdapter[Any]:
    """Build a RabbitMQ-backed queue port for the given spec."""
    rabbitmq_client = context.dep(RabbitMQClientDepKey)
    codec = RabbitMQQueueCodec(model=spec.model)

    return RabbitMQQueueAdapter(
        client=rabbitmq_client,
        codec=codec,
        namespace=spec.namespace,
    )
