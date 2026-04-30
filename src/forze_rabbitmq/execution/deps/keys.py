"""Dependency keys for RabbitMQ-related services."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import RabbitMQClientPort

# ----------------------- #

RabbitMQClientDepKey: DepKey[RabbitMQClientPort] = DepKey("rabbitmq_client")
"""Key used to register a RabbitMQ client (single-DSN or routed) in the deps container."""
