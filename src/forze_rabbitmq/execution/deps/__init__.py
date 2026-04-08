"""RabbitMQ dependency keys and module."""

from .configs import RabbitMQQueueConfig
from .keys import RabbitMQClientDepKey
from .module import RabbitMQDepsModule

# ----------------------- #

__all__ = ["RabbitMQDepsModule", "RabbitMQClientDepKey", "RabbitMQQueueConfig"]
