"""RabbitMQ execution wiring for the application kernel."""

from .deps import RabbitMQClientDepKey, RabbitMQDepsModule
from .lifecycle import rabbitmq_lifecycle_step

# ----------------------- #

__all__ = [
    "RabbitMQDepsModule",
    "RabbitMQClientDepKey",
    "rabbitmq_lifecycle_step",
]
