"""Private tenancy warning descriptors for SQS deps module."""

from forze.application.contracts.tenancy import IntegrationRouteWarning

from .configs import SQSQueueConfig

# ----------------------- #


def _queue_warning(*, kind: str) -> IntegrationRouteWarning[SQSQueueConfig]:
    return IntegrationRouteWarning(
        kind=kind,
        tenant_aware=lambda config: config.tenant_aware,
        named_fields=lambda config: [("namespace", config.namespace)],
    )


SQS_QUEUE_READER_WARNING = _queue_warning(kind="queue_reader")
SQS_QUEUE_WRITER_WARNING = _queue_warning(kind="queue_writer")
