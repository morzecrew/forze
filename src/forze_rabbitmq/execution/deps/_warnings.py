"""Private tenancy warning descriptors for RabbitMQ deps module."""

from forze.application.contracts.tenancy import IntegrationRouteWarning

from .configs import RabbitMQQueueConfig

# ----------------------- #


def _queue_warning(*, kind: str) -> IntegrationRouteWarning[RabbitMQQueueConfig]:
    return IntegrationRouteWarning(
        kind=kind,
        tenant_aware=lambda config: config.tenant_aware,
        named_fields=lambda config: [("namespace", config.namespace)],
    )


RABBITMQ_QUEUE_READER_WARNING = _queue_warning(kind="queue_reader")
RABBITMQ_QUEUE_WRITER_WARNING = _queue_warning(kind="queue_writer")
