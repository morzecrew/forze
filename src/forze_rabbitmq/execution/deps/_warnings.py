"""Private tenancy warning descriptors for RabbitMQ deps module."""

from forze.application.contracts.tenancy import namespace_route_warning

from .configs import RabbitMQQueueConfig

# ----------------------- #

RABBITMQ_QUEUE_READER_WARNING = namespace_route_warning(
    RabbitMQQueueConfig, kind="queue_reader"
)
RABBITMQ_QUEUE_WRITER_WARNING = namespace_route_warning(
    RabbitMQQueueConfig, kind="queue_writer"
)
