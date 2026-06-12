from datetime import datetime, timedelta
from types import MappingProxyType
from typing import Final, Mapping, final

import attrs

# ----------------------- #

_EMPTY_HEADERS: Final[Mapping[str, str]] = MappingProxyType({})

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamMessage[M]:
    """Message as read from or written to a stream backend."""

    stream: str
    """Logical stream name or topic."""

    id: str
    """Backend-specific identifier for the message."""

    payload: M
    """Structured payload carried by the message."""

    type: str | None = None
    """Optional message type or category."""

    timestamp: datetime | None = None
    """Optional timestamp associated with the message."""

    key: str | None = None
    """Optional partitioning key for the message."""

    headers: Mapping[str, str] = _EMPTY_HEADERS
    """String-to-string transport metadata carried alongside the payload.

    Propagated best-effort via the backend's native metadata channel; not
    part of the payload contract. See
    :mod:`forze.application.contracts.envelope` for the well-known keys.
    """


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PendingEntry:
    """Delivered-but-unacknowledged entry of a stream consumer group.

    Returned by :meth:`~forze.application.contracts.stream.StreamGroupQueryPort.pending`
    as a point-in-time snapshot of the group's pending-entries list. Inspection
    only: looking at a pending entry never changes its owner, idle time, or
    delivery count — use
    :meth:`~forze.application.contracts.stream.StreamGroupQueryPort.claim` to
    take ownership of stranded entries.
    """

    id: str
    """Backend-specific identifier of the pending entry."""

    consumer: str
    """Consumer currently owning the entry (the one it was last delivered to)."""

    idle: timedelta
    """Elapsed time since the entry was last delivered (read or claimed)."""

    delivery_count: int | None = None
    """How many times the entry has been delivered, including the initial group
    read and every subsequent claim; ``None`` when the backend does not track it."""
