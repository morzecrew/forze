"""RabbitMQ integration for Forze."""

from ._compat import require_rabbitmq

require_rabbitmq()

# ....................... #

from .execution import (
    RabbitMQClientDepKey,
    RabbitMQDepsModule,
    RabbitMQQueueConfig,
    routed_rabbitmq_lifecycle_step,
    rabbitmq_lifecycle_step,
)
from .kernel.platform import (
    RabbitMQClient,
    RabbitMQClientPort,
    RabbitMQConfig,
    RabbitMQQueueMessage,
    RoutedRabbitMQClient,
)

# ----------------------- #

__all__ = [
    "RabbitMQClient",
    "RabbitMQClientPort",
    "RabbitMQConfig",
    "RabbitMQQueueMessage",
    "RoutedRabbitMQClient",
    "RabbitMQClientDepKey",
    "RabbitMQDepsModule",
    "rabbitmq_lifecycle_step",
    "routed_rabbitmq_lifecycle_step",
    "RabbitMQQueueConfig",
]
