from datetime import datetime
from typing import final

import attrs

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class QueueMessage[M]:
    """Message as read from or written to a queue backend."""

    queue: str
    """Logical queue name or channel."""

    id: str
    """Broker message identifier.

    Stable across redeliveries of the same message and correlatable with the
    identifier returned by enqueue (RabbitMQ publisher message id; SQS broker
    ``MessageId``) — safe to use for consumer-side deduplication. Backends
    accept it in ``ack``/``nack`` calls.
    """

    payload: M
    """Structured payload carried by the message."""

    type: str | None = None
    """Optional message type or category."""

    enqueued_at: datetime | None = None
    """Optional timestamp associated with the message."""

    key: str | None = None
    """Optional partitioning key for the message."""
