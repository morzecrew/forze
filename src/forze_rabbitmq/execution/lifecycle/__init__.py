"""RabbitMQ lifecycle steps (client pool startup and shutdown)."""

from .pool import (
    RabbitMQShutdownHook,
    RabbitMQStartupHook,
    rabbitmq_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "RabbitMQShutdownHook",
    "RabbitMQStartupHook",
    "rabbitmq_lifecycle_step",
]
