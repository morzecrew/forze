from collections.abc import AsyncGenerator, Awaitable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import (
    Protocol,
    runtime_checkable,
)

from .value_objects import (
    AckGroupDepth,
    ConsumerLag,
    OffsetReset,
    PendingEntry,
    StreamMessage,
    StreamPosition,
)

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
class AckStreamGroupQueryPort[M](Protocol):
    """Consumer-group stream reads with per-message acknowledgment and recovery.

    The **ack** sub-model of the stream family (Redis-class): each entry is
    acknowledged individually by id and stranded entries are recovered by an
    explicit :meth:`claim`. Its sibling is
    :class:`CommitStreamGroupQueryPort`, the **commit** sub-model (offset-log /
    Kafka-class), where a single committed offset acknowledges every message up
    to it. The ``Ack``/``Commit`` prefix names the acknowledgment discipline —
    the one axis on which the two consumer-group ports differ.

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
class AckStreamGroupAdminPort(Protocol):
    """Control-plane provisioning for ack-stream consumer groups.

    Kept **separate** from the data-plane :class:`AckStreamGroupQueryPort` (read /
    ack / claim / pending): group creation mutates shared topology and runs once
    at startup, so a request-path consumer never sees it. Mirrors the framework's
    management/data split (e.g. ``TenantManagementPort`` vs ``TenantResolverPort``).
    """

    def ensure_group(
        self,
        group: str,
        stream: str,
        *,
        start_id: str = "$",
    ) -> Awaitable[None]:
        """Idempotently create consumer *group* on *stream* (creating the stream if absent).

        Must run before any group read on a backend that requires explicit group
        creation (Redis: ``XGROUP CREATE … MKSTREAM``); a no-op if the group
        already exists. Run it **early in startup, before anything publishes**, so
        a ``start_id`` of ``"$"`` (deliver only entries added after creation —
        the live default) does not miss messages produced in the startup window.
        Pass ``"0"`` to start from the beginning of the stream.

        :param group: Consumer group name.
        :param stream: Stream the group consumes.
        :param start_id: Where a freshly-created group starts.
        """
        ...  # pragma: no cover

    # ....................... #

    def depth(self, group: str, stream: str) -> Awaitable[AckGroupDepth]:
        """Snapshot *group*'s outstanding work on *stream* — backlog + pending.

        The ack family's observability verb, and the reason it lives on the **admin**
        port: it observes the group as a whole (control-plane), while the query port's
        :meth:`~AckStreamGroupQueryPort.pending` enumerates entries for a consumer doing
        recovery (data-plane). Read-only — never changes ownership, idle clocks, or
        delivery counts. A quiesce sweep polls it until
        :attr:`~forze.application.contracts.stream.value_objects.AckGroupDepth.at_rest`;
        a depth gauge exports it.

        Raises ``not_found`` when the group does not exist on *stream* — an absent group
        is a wiring gap (the ensure-group step did not run), not an empty one.
        """
        ...  # pragma: no cover

    # ....................... #

    def trim_acknowledged(self, stream: str) -> Awaitable[int]:
        """Trim entries that **every** consumer group on *stream* has delivered and acked.

        Retention maintenance for the ack family: an approximate length cap bounds a
        stream bluntly, this trims *precisely* — the floor is computed from group state
        (each group's lowest pending entry, or its delivered horizon when nothing is
        pending), so an entry a group has not yet been delivered, or has been delivered
        but not acknowledged, is never removed. Always safe to run, on any interval, on
        any number of nodes concurrently (idempotent, monotonic).

        Fail-safe by construction: a stream with **no** groups trims nothing (there is no
        horizon to trust), and a group added later starts from wherever its ``start_id``
        put it, unaffected by earlier trims below that point.

        :returns: the number of entries removed.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class CommitStreamGroupQueryPort[M](Protocol):
    """Consume an offset-committed, partitioned log through a consumer group.

    The **commit** sub-model of the stream family (Kafka-class), sibling to the
    **ack** :class:`AckStreamGroupQueryPort`. Delivery is at-least-once:
    messages already read but not yet committed are redelivered after a crash or
    rebalance, so handlers must dedup (see the inbox). Recovery is
    broker-coordinated — partitions are reassigned across live group members
    automatically; there is no per-message :meth:`~AckStreamGroupQueryPort.claim`.
    Committing a :class:`StreamPosition` acknowledges every message up to and
    including it on that partition (a high-water mark, not a set of ids).

    Partitions and rebalancing are hidden in the adapter; the contract speaks
    ``read`` / ``commit`` / positions, not a broker's poll/assign vocabulary.
    """

    def read(
        self,
        group: str,
        consumer: str,
        topics: Sequence[str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> Awaitable[list[StreamMessage[M]]]:
        """Read a batch for *consumer* in *group* across its assigned partitions of *topics*.

        The group tracks committed offsets server-side, so — unlike the ack
        port's ``stream_mapping`` cursor — the caller passes only the *topics* to
        subscribe to and the backend returns the next uncommitted messages.
        """
        ...  # pragma: no cover

    # ....................... #

    def tail(
        self,
        group: str,
        consumer: str,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        """Continuously yield messages for *consumer* in *group* across *topics*."""
        ...  # pragma: no cover

    # ....................... #

    def commit(
        self,
        group: str,
        positions: Sequence[StreamPosition],
    ) -> Awaitable[None]:
        """Commit processed *positions*; the highest offset per ``(stream, partition)`` wins.

        Committing acknowledges every message up to and including each position
        on its partition. Build a position from a processed message's typed
        fields with :meth:`StreamPosition.from_message`.
        """
        ...  # pragma: no cover

    # ....................... #

    def seek_to_committed(
        self,
        group: str,
        topics: Sequence[str],
    ) -> Awaitable[None]:
        """Rewind *group*'s in-memory read position on *topics* to its committed offset.

        Called on an **abort / pause** path so a partially-read, uncommitted batch
        is re-fetched from the last committed offset on the next read (or a
        supervised restart) rather than silently skipped. An adapter that advances
        an in-memory position on ``read`` (Kafka-class, whose pooled consumer moves
        past a whole fetched batch) must undo that advance here; a backend whose
        read position *is* the committed cursor (no separate in-memory position)
        implements this as a no-op.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class CommitStreamGroupAdminPort(Protocol):
    """Control-plane provisioning, replay, and lag inspection for offset-log streams.

    Kept **separate** from the data-plane :class:`CommitStreamGroupQueryPort`
    (read / commit), mirroring the ack sub-model's
    :class:`AckStreamGroupAdminPort` and the framework's management/data split.
    """

    def ensure_topic(
        self,
        stream: str,
        *,
        partitions: int,
        replication: int = 1,
        config: Mapping[str, str] | None = None,
    ) -> Awaitable[None]:
        """Idempotently create *stream* with *partitions* (no-op if present).

        *partitions* and *replication* are the two offset-log-shaped knobs the
        ack family never needed; a backend that auto-creates topics may treat
        this as a no-op or a config assertion.
        """
        ...  # pragma: no cover

    # ....................... #

    def ensure_group(
        self,
        group: str,
        topics: Sequence[str],
        *,
        start: OffsetReset = OffsetReset.LATEST,
    ) -> Awaitable[None]:
        """Idempotently initialize *group* on *topics*; *start* is the first-time position.

        *start* mirrors a broker's first-consume default (earliest / latest);
        it applies only when the group has no committed offset yet.
        """
        ...  # pragma: no cover

    # ....................... #

    def reset_offsets(
        self,
        group: str,
        stream: str,
        *,
        to: OffsetReset,
    ) -> Awaitable[None]:
        """Seek *group* on *stream* to *to* — replay / skip.

        Requires :attr:`CommitStreamGroupCapabilities.supports_replay`; a backend
        that cannot seek fails closed at this call (``stream.replay_unsupported``).
        """
        ...  # pragma: no cover

    # ....................... #

    def lag(
        self,
        group: str,
        stream: str | None = None,
    ) -> Awaitable[list[ConsumerLag]]:
        """Per-partition committed / end / lag for *group* — the observability analog of ``pending()``.

        Scopes to *stream* when given, else reports every subscribed stream.
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
