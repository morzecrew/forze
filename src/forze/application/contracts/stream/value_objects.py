from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Any, ClassVar, Final, Mapping, Self, final

import attrs

from forze.base.exceptions import exc

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

    partition: int | None = None
    """Partition the message belongs to, for offset-log backends (Kafka-class).

    ``None`` for the ack sub-model (Redis-class), which has no partitions.
    Populated by a :class:`~forze.application.contracts.stream.CommitStreamGroupQueryPort`
    adapter alongside :attr:`offset`; together they form the message's
    :class:`StreamPosition`.
    """

    offset: int | None = None
    """Per-partition offset of the message, for offset-log backends (Kafka-class).

    ``None`` for the ack sub-model. Committing this offset acknowledges every
    message up to and including it on :attr:`partition` (a high-water mark). See
    :class:`StreamPosition`.
    """

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

    Returned by :meth:`~forze.application.contracts.stream.AckStreamGroupQueryPort.pending`
    as a point-in-time snapshot of the group's pending-entries list. Inspection
    only: looking at a pending entry never changes its owner, idle time, or
    delivery count — use
    :meth:`~forze.application.contracts.stream.AckStreamGroupQueryPort.claim` to
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


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StreamPosition:
    """A committed position in an offset-log stream: ``(stream, partition, offset)``.

    The commit unit of the offset-log sub-model
    (:class:`~forze.application.contracts.stream.CommitStreamGroupQueryPort`).
    Committing a position acknowledges every message up to and including
    *offset* on *partition* — a per-partition high-water mark, not a set of
    ids. Built from a message's typed :attr:`StreamMessage.partition` /
    :attr:`StreamMessage.offset` fields via :meth:`from_message` (no
    stringly-typed parsing); :meth:`__str__` renders the canonical id form the
    adapter also stores in :attr:`StreamMessage.id`.
    """

    stream: str
    """Logical stream (topic) name."""

    partition: int
    """Partition within the stream."""

    offset: int
    """Offset of the message within the partition."""

    # ....................... #

    @classmethod
    def from_message(cls, message: StreamMessage[Any]) -> Self:
        """Read a position from *message*'s typed ``partition`` / ``offset`` fields.

        Raises :func:`~forze.base.exceptions.exc.precondition` if either is
        ``None`` — i.e. the message did not come from an offset-log backend.
        """

        if message.partition is None or message.offset is None:
            raise exc.precondition(
                f"Message {message.id!r} on stream {message.stream!r} carries no "
                "partition/offset; it is not from an offset-log backend",
                code="stream.position_missing",
            )

        return cls(
            stream=message.stream,
            partition=message.partition,
            offset=message.offset,
        )

    # ....................... #

    def __str__(self) -> str:
        return f"{self.stream}:{self.partition}:{self.offset}"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class UndecodableStreamPayload:
    """Marker payload for an offset-log record whose value could not be decoded.

    An offset-log adapter (Kafka-class) that decodes a record's value **inside**
    its read path must not let a single malformed record raise out of ``read``:
    the pooled consumer has already advanced its in-memory position past the whole
    batch, so a raised decode error would abort the batch and a later commit could
    skip the unprocessed records silently. Instead the adapter substitutes this
    marker for the model payload, keeping the message's ``(partition, offset)`` /
    ``id`` intact, so the consumer runner sees a poison record it can pause-and-alert
    on (leaving the offset uncommitted for redelivery) exactly like a decrypt/decode
    poison — never a skip. The raw bytes and error are carried for operator triage.
    """

    raw: bytes
    """The undecodable record value bytes, verbatim."""

    error: str
    """String form of the decode error, for logs / operator inspection."""


# ....................... #


class OffsetResetKind(StrEnum):
    """Where an offset-log group's cursor is (re)positioned."""

    EARLIEST = "earliest"
    """The start of the log — replay everything retained."""

    LATEST = "latest"
    """The tail of the log — skip everything already produced."""

    TIMESTAMP = "timestamp"
    """The first offset at or after :attr:`OffsetReset.timestamp`."""

    OFFSET = "offset"
    """An explicit :attr:`OffsetReset.offset`."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OffsetReset:
    """A target position for ``ensure_group(start=…)`` / ``reset_offsets(to=…)``.

    An enum-plus-payload: :attr:`kind` selects the semantic and the optional
    :attr:`timestamp` / :attr:`offset` carry the payload for the two positional
    kinds. Use the constructors — :attr:`EARLIEST`, :attr:`LATEST`,
    :meth:`at_timestamp`, :meth:`at_offset` — rather than building instances by
    hand.
    """

    EARLIEST: ClassVar[OffsetReset]
    """Seek to the start of the log (replay everything retained)."""

    LATEST: ClassVar[OffsetReset]
    """Seek to the tail of the log (skip everything already produced)."""

    kind: OffsetResetKind
    """Which repositioning semantic this target selects."""

    timestamp: datetime | None = None
    """Payload for :attr:`OffsetResetKind.TIMESTAMP`; ``None`` otherwise."""

    offset: int | None = None
    """Payload for :attr:`OffsetResetKind.OFFSET`; ``None`` otherwise."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.kind is OffsetResetKind.TIMESTAMP and self.timestamp is None:
            raise exc.validation("OffsetReset.at_timestamp requires a timestamp")

        if self.kind is OffsetResetKind.OFFSET and self.offset is None:
            raise exc.validation("OffsetReset.at_offset requires an offset")

    # ....................... #

    @classmethod
    def at_timestamp(cls, timestamp: datetime) -> Self:
        """Seek to the first offset at or after *timestamp*."""

        return cls(kind=OffsetResetKind.TIMESTAMP, timestamp=timestamp)

    # ....................... #

    @classmethod
    def at_offset(cls, offset: int) -> Self:
        """Seek to an explicit *offset*."""

        return cls(kind=OffsetResetKind.OFFSET, offset=offset)


OffsetReset.EARLIEST = OffsetReset(kind=OffsetResetKind.EARLIEST)
OffsetReset.LATEST = OffsetReset(kind=OffsetResetKind.LATEST)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConsumerLag:
    """Per-partition consumer-group lag: committed vs end offset.

    The offset-log observability analog of the ack sub-model's
    :class:`PendingEntry` — instead of enumerating in-flight entries, an
    offset-log reports how far a group's committed cursor trails the log's end
    on each partition. Returned by
    :meth:`~forze.application.contracts.stream.CommitStreamGroupAdminPort.lag`.
    """

    stream: str
    """Logical stream (topic) name."""

    partition: int
    """Partition the lag is reported for."""

    committed_offset: int
    """Highest offset the group has committed on this partition."""

    end_offset: int
    """Offset just past the last produced message on this partition."""

    @property
    def lag(self) -> int:
        """Uncommitted messages on this partition (``max(0, end - committed)``).

        Derived, never stored — so an adapter cannot report a lag inconsistent
        with its committed/end offsets.
        """

        return max(0, self.end_offset - self.committed_offset)
