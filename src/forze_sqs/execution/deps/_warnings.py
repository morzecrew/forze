"""Private tenancy warning descriptors for SQS deps module."""

from forze.application.contracts.tenancy import namespace_route_warning

from .configs import SQSQueueConfig

# ----------------------- #

SQS_QUEUE_READER_WARNING = namespace_route_warning(SQSQueueConfig, kind="queue_reader")
SQS_QUEUE_WRITER_WARNING = namespace_route_warning(SQSQueueConfig, kind="queue_writer")
