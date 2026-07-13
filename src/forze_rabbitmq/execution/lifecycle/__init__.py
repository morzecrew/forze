"""RabbitMQ lifecycle steps (client pool startup and shutdown)."""

from .pool import (
    RabbitMQShutdownHook,
    RabbitMQStartupHook,
    rabbitmq_lifecycle_step,
    routed_rabbitmq_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "RabbitMQShutdownHook",
    "RabbitMQStartupHook",
    "routed_rabbitmq_lifecycle_step",
    "rabbitmq_lifecycle_step",
]
