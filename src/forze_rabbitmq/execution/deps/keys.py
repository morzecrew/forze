"""Dependency keys for RabbitMQ-related services."""

from forze.application.contracts.deps import DepKey

from ...kernel.platform import RabbitMQClient

# ----------------------- #

RabbitMQClientDepKey: DepKey[RabbitMQClient] = DepKey("rabbitmq_client")
"""Key used to register the :class:`RabbitMQClient` in the deps container."""
