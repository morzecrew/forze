"""RabbitMQ dependency keys and module."""

from .configs import RabbitMQQueueConfig
from .factories import ConfigurableRabbitMQQueueRead, ConfigurableRabbitMQQueueWrite
from .keys import RabbitMQClientDepKey
from .module import RabbitMQDepsModule

# ----------------------- #

__all__ = [
    "RabbitMQDepsModule",
    "RabbitMQClientDepKey",
    "RabbitMQQueueConfig",
    "ConfigurableRabbitMQQueueRead",
    "ConfigurableRabbitMQQueueWrite",
]
