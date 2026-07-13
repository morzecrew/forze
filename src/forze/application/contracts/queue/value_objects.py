from collections.abc import Mapping
from datetime import datetime
from types import MappingProxyType
from typing import Final, final

import attrs

# ----------------------- #

_EMPTY_HEADERS: Final[Mapping[str, str]] = MappingProxyType({})

# ....................... #


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
    """Optional partition/correlation token, round-tripped from enqueue.

    Portably an opaque grouping token; what the broker does with it differs
    per backend — see the canonical table on
    :meth:`~forze.application.contracts.queue.QueueCommandPort.enqueue`.
    """

    headers: Mapping[str, str] = _EMPTY_HEADERS
    """String-to-string transport metadata carried alongside the payload.

    Propagated best-effort via the backend's native metadata channel; not
    part of the payload contract. See
    :mod:`forze.application.contracts.envelope` for the well-known keys.
    """

    delivery_count: int | None = None
    """Approximate number of deliveries of this message, **including** this one.

    ``None`` when the backend cannot report it. Backend-specific accuracy:
    SQS reports ``ApproximateReceiveCount`` exactly; RabbitMQ approximates
    from ``x-death`` history and the ``redelivered`` flag (see the RabbitMQ
    client docs); the mock adapter counts exactly.
    """
