from datetime import datetime, timedelta
from typing import (
    AsyncGenerator,
    Awaitable,
    Mapping,
    Protocol,
    Sequence,
    runtime_checkable,
)

from .value_objects import PendingEntry, StreamMessage

# ----------------------- #


@runtime_checkable
class StreamQueryPort[M](Protocol):
    """Contract for querying messages from one or more streams."""

    def read(
        self,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[list[StreamMessage[M]]]:
        """Read a batch of messages from the streams in *stream_mapping*."""
        ...  # pragma: no cover

    # ....................... #

    def tail(
        self,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        """Continuously yield new messages from the mapped streams."""
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class StreamGroupQueryPort[M](Protocol):
    """Contract for consumer-group-based stream reads, acknowledgments, and recovery.

    Group delivery is exclusive: each entry is delivered to exactly one consumer
    per group and stays pending for that consumer until acknowledged via
    :meth:`ack`. A consumer that crashes after a read therefore strands its
    pending entries — no group read ever redelivers another consumer's pending
    entries. Recovery is explicit: :meth:`pending` inspects the outstanding
    entries and :meth:`claim` transfers stranded ones to a live consumer.
    """

    def read(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[list[StreamMessage[M]]]:
        """Read pending messages for *consumer* in *group*."""
        ...  # pragma: no cover

    # ....................... #

    def tail(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        """Continuously yield new messages for *consumer* in *group*."""
        ...  # pragma: no cover

    # ....................... #

    def ack(self, group: str, stream: str, ids: Sequence[str]) -> Awaitable[int]:
        """Acknowledge processed messages within *group*."""
        ...  # pragma: no cover

    # ....................... #

    def claim(
        self,
        group: str,
        consumer: str,
        stream: str,
        *,
        idle: timedelta,
        limit: int | None = None,
    ) -> Awaitable[list[StreamMessage[M]]]:
        """Transfer ownership of entries pending at least *idle* to *consumer*.

        Crash recovery for consumer groups (``XAUTOCLAIM`` semantics): a
        consumer that reads a batch and crashes before :meth:`ack` leaves
        those entries pending under its name forever. Any worker can recover
        them by calling ``claim`` with the group's processing deadline as
        *idle* — every entry whose last delivery (read or claim, by **any**
        consumer, including *consumer* itself) happened at least *idle* ago
        is reassigned to *consumer* and returned for reprocessing.

        Claimed entries are **redeliveries**: consumers must process them
        idempotently and deduplicate as for any redelivered message. They
        remain pending — now for *consumer* — until acknowledged via
        :meth:`ack`, their delivery count increments, and claiming resets the
        idle clock, so a claimer that crashes in turn leaves them claimable
        again once another *idle* elapses. Entries pending for less than
        *idle* are never touched.

        :param group: Consumer group name.
        :param consumer: Consumer receiving ownership of the claimed entries.
        :param stream: Stream whose pending entries are scanned.
        :param idle: Minimum elapsed time since last delivery for an entry to
            be claimable.
        :param limit: Maximum number of entries to claim; ``None`` claims
            every eligible entry.
        :returns: The claimed messages, oldest first.
        """
        ...  # pragma: no cover

    # ....................... #

    def pending(
        self,
        group: str,
        stream: str,
        *,
        limit: int | None = None,
    ) -> Awaitable[list[PendingEntry]]:
        """Inspect *group*'s delivered-but-unacknowledged entries on *stream*.

        Returns one :class:`~forze.application.contracts.stream.value_objects.PendingEntry`
        per outstanding entry — current owner, time since last delivery, and
        delivery count — oldest first. Read-only: inspection never changes
        ownership, idle clocks, or delivery counts. Use it to watch consumer
        lag and decide when stranded entries are due for :meth:`claim`.

        :param group: Consumer group name.
        :param stream: Stream whose pending entries are listed.
        :param limit: Maximum number of entries to return; ``None`` returns
            every pending entry.
        :returns: Snapshot of the pending entries, oldest first.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class StreamCommandPort[M](Protocol):
    """Contract for appending messages to a stream backend."""

    def append(
        self,
        stream: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Awaitable[str]:
        """Append a message to *stream* and return its identifier.

        :param headers: String-to-string transport metadata, propagated
            best-effort via the backend's native metadata channel and surfaced
            on read messages as ``StreamMessage.headers``. Not part of the
            payload contract.
        """
        ...  # pragma: no cover
