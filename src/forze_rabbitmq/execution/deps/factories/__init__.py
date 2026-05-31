"""RabbitMQ dependency factories."""

from .read import ConfigurableRabbitMQQueueRead
from .write import ConfigurableRabbitMQQueueWrite

# ----------------------- #

__all__ = [
    "ConfigurableRabbitMQQueueRead",
    "ConfigurableRabbitMQQueueWrite",
]
