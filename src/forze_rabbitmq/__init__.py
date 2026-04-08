"""RabbitMQ integration for Forze."""

from ._compat import require_rabbitmq

require_rabbitmq()

# ....................... #

from .execution import (
    RabbitMQClientDepKey,
    RabbitMQDepsModule,
    RabbitMQQueueConfig,
    rabbitmq_lifecycle_step,
)
from .kernel.platform import RabbitMQClient, RabbitMQConfig

# ----------------------- #

__all__ = [
    "RabbitMQClient",
    "RabbitMQConfig",
    "RabbitMQClientDepKey",
    "RabbitMQDepsModule",
    "rabbitmq_lifecycle_step",
    "RabbitMQQueueConfig",
]
