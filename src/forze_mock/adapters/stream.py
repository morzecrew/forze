"""In-memory stream adapters."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta
from typing import (
    Any,
    AsyncGenerator,
    Mapping,
    Sequence,
    cast,
    final,
)

import attrs
from pydantic import BaseModel

from forze.application.contracts.stream import (
    AckStreamGroupAdminPort,
    AckStreamGroupQueryPort,
    CommitStreamGroupAdminPort,
    CommitStreamGroupCapabilities,
    CommitStreamGroupQueryPort,
    ConsumerLag,
    OffsetReset,
    OffsetResetKind,
    PendingEntry,
    StreamCommandPort,
    StreamMessage,
    StreamPosition,
    StreamQueryPort,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow
from forze.base.serialization import (
    ModelCodec,
)
from forze_mock.adapters.queue import (
    _sleep_interval,  # type: ignore[reportPrivateUsage]
)
from forze_mock.query._types import M
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStreamAdapter(MockTenancyMixin, StreamQueryPort[M], StreamCommandPort[M]):
    """In-memory stream adapter with monotonic message identifiers."""

    state: MockState
    namespace: str
    codec: ModelCodec[M, Any]

    # ....................... #

    def _ns(self) -> str:
        return self._partitioned_namespace(self.namespace)

    def _stream_store(self) -> dict[str, list[StreamMessage[M]]]:
        return cast(
            dict[str, list[StreamMessage[M]]],
            self.state.streams.setdefault(self._ns(), {}),
        )

    # ....................... #

    def _id_to_int(self, value: str) -> int:
        suffix = value.rsplit("-", 1)[-1]
        try:
            return int(suffix)
        except ValueError:
            return 0

    # ....................... #

    async def append(
        self,
        stream: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        timestamp: datetime | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        message_id = self.state.next_id("stream")
        message = StreamMessage(
            stream=stream,
            id=message_id,
            payload=payload,
            type=type,
            key=key,
            timestamp=timestamp or utcnow(),
            headers=dict(headers) if headers else {},
        )
        with self.state.lock:
            self._stream_store().setdefault(stream, []).append(message)
        return message_id

    # ....................... #

    async def read(
        self,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[M]]:
        del timeout
        out: list[StreamMessage[M]] = []
        with self.state.lock:
            for stream, last_id in stream_mapping.items():
                log = self._stream_store().setdefault(stream, [])
                last_num = self._id_to_int(last_id)
                for msg in log:
                    if self._id_to_int(msg.id) > last_num:
                        out.append(msg)
                        if limit is not None and len(out) >= limit:
                            return out
        return out

    # ....................... #

    async def tail(
        self,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        cursor = dict(stream_mapping)
        while True:
            messages = await self.read(cursor, timeout=timeout)
            for message in messages:
                cursor[message.stream] = message.id
                yield message
            if not messages:
                await asyncio.sleep(_sleep_interval(timeout))


_GROUPS_KEY = "\x00__consumer_groups__"
"""Reserved stream-store key holding consumer-group bookkeeping.

The ``\\x00`` prefix cannot collide with a real stream name and the plain
adapter never iterates store keys, so the entry stays invisible to
:class:`MockStreamAdapter`.
"""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _MockPendingMeta:
    """Per-entry pending-entries-list record, mirroring the Redis PEL fields."""

    consumer: str
    """Consumer currently owning the entry."""

    delivered_at: datetime
    """Instant of the last delivery (group read or claim); the idle clock origin."""

    delivery_count: int = 1
    """Deliveries so far: ``1`` on the initial group read, ``+1`` per claim."""


# ....................... #


@attrs.define(slots=True, kw_only=True)
class _MockGroupState:
    """Per ``(group, stream)`` consumer-group bookkeeping.

    Mirrors the observable Redis consumer-group state: the group's
    last-delivered id and the pending-entries map (entry id ->
    :class:`_MockPendingMeta` with owner, last delivery time, and delivery
    count).
    """

    last_delivered: int = 0
    pending: dict[str, _MockPendingMeta] = attrs.field(factory=dict)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockAckStreamGroupAdapter[M: BaseModel](AckStreamGroupQueryPort[M]):
    """In-memory stream group adapter mirroring Redis ``XREADGROUP`` semantics.

    Each entry is delivered to exactly one consumer per group: a ``">"``
    cursor reads entries after the group's last-delivered id, advances it,
    and records the entries as pending for the reading consumer.  A concrete
    id cursor re-reads the consumer's *own* pending entries after that id
    (the ``XREADGROUP`` history view) without touching the pending metadata.
    :meth:`ack` removes entries from the pending map.

    Recovery mirrors ``XAUTOCLAIM``/``XPENDING``: per-entry pending metadata
    tracks the owning consumer, last delivery instant, and delivery count.
    :meth:`claim` reassigns entries idle for at least the given threshold —
    bumping the delivery count and resetting the idle clock — and
    :meth:`pending` reports the computed idle times.  Delivery instants come
    from the bound
    :class:`~forze.base.primitives.TimeSource` (via
    :func:`~forze.base.primitives.utcnow`), so idle-based recovery is
    deterministic under a frozen clock, like the queue visibility timeout.
    """

    stream: MockStreamAdapter[M]
    state: MockState
    namespace: str

    # ....................... #

    def _group_state(self, group: str, stream: str) -> _MockGroupState:
        store = cast(dict[str, Any], self.stream._stream_store())  # type: ignore[reportPrivateUsage]
        holder = cast(list[Any], store.setdefault(_GROUPS_KEY, [{}]))
        groups = cast(dict[tuple[str, str], _MockGroupState], holder[0])
        key = (group, stream)

        if key not in groups:
            groups[key] = _MockGroupState()

        return groups[key]

    # ....................... #

    async def read(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[M]]:
        del timeout
        out: list[StreamMessage[M]] = []
        now = utcnow()

        with self.state.lock:
            for stream, cursor in stream_mapping.items():
                gs = self._group_state(group, stream)
                entries: list[StreamMessage[M]] = self.stream._stream_store().setdefault(  # type: ignore[reportPrivateUsage]
                    stream,
                    [],
                )

                if cursor == ">":
                    # New entries: deliver once per group, record as pending.
                    for msg in entries:
                        num = self.stream._id_to_int(msg.id)  # type: ignore[reportPrivateUsage]

                        if num <= gs.last_delivered:
                            continue

                        gs.last_delivered = num
                        gs.pending[msg.id] = _MockPendingMeta(
                            consumer=consumer,
                            delivered_at=now,
                        )
                        out.append(msg)

                        if limit is not None and len(out) >= limit:
                            return out

                else:
                    # History view: the consumer's own pending entries
                    # strictly after the given id (acked entries excluded).
                    # Like Redis, the history read leaves delivery time and
                    # count untouched.
                    last_num = self.stream._id_to_int(cursor)  # type: ignore[reportPrivateUsage]

                    for msg in entries:
                        if self.stream._id_to_int(msg.id) <= last_num:  # type: ignore[reportPrivateUsage]
                            continue

                        meta = gs.pending.get(msg.id)

                        if meta is None or meta.consumer != consumer:
                            continue

                        out.append(msg)

                        if limit is not None and len(out) >= limit:
                            return out

        return out

    # ....................... #

    async def tail(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        cursor = dict(stream_mapping)
        while True:
            messages = await self.read(group, consumer, cursor, timeout=timeout)
            for message in messages:
                cursor[message.stream] = message.id
                yield message
            if not messages:
                await asyncio.sleep(_sleep_interval(timeout))

    # ....................... #

    async def ack(self, group: str, stream: str, ids: Sequence[str]) -> int:
        key = (self.stream._ns(), group, stream)  # type: ignore[reportPrivateUsage]
        with self.state.lock:
            gs = self._group_state(group, stream)
            removed: list[str] = [i for i in ids if gs.pending.pop(i, None) is not None]
            self.state.stream_ack.setdefault(key, set()).update(removed)
            return len(removed)

    # ....................... #

    # ....................... #

    async def claim(
        self,
        group: str,
        consumer: str,
        stream: str,
        *,
        idle: timedelta,
        limit: int | None = None,
    ) -> list[StreamMessage[M]]:
        out: list[StreamMessage[M]] = []
        now = utcnow()

        with self.state.lock:
            gs = self._group_state(group, stream)
            entries: list[StreamMessage[M]] = self.stream._stream_store().setdefault(  # type: ignore[reportPrivateUsage]
                stream,
                [],
            )

            # Walk the log (id order) so claims come back oldest first.
            for msg in entries:
                meta = gs.pending.get(msg.id)

                if meta is None:
                    continue

                # XAUTOCLAIM threshold: claim once at least *idle* elapsed.
                if now - meta.delivered_at < idle:
                    continue

                # Reassign: new owner, delivery count bumped, idle clock reset.
                gs.pending[msg.id] = _MockPendingMeta(
                    consumer=consumer,
                    delivered_at=now,
                    delivery_count=meta.delivery_count + 1,
                )
                out.append(msg)

                if limit is not None and len(out) >= limit:
                    break

        return out

    # ....................... #

    async def pending(
        self,
        group: str,
        stream: str,
        *,
        limit: int | None = None,
    ) -> list[PendingEntry]:
        out: list[PendingEntry] = []
        now = utcnow()

        with self.state.lock:
            gs = self._group_state(group, stream)
            entries: list[StreamMessage[M]] = self.stream._stream_store().setdefault(  # type: ignore[reportPrivateUsage]
                stream,
                [],
            )

            for msg in entries:
                meta = gs.pending.get(msg.id)

                if meta is None:
                    continue

                out.append(
                    PendingEntry(
                        id=msg.id,
                        consumer=meta.consumer,
                        idle=max(timedelta(0), now - meta.delivered_at),
                        delivery_count=meta.delivery_count,
                    )
                )

                if limit is not None and len(out) >= limit:
                    break

        return out


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockAckStreamGroupAdminAdapter[M: BaseModel](AckStreamGroupAdminPort):
    """In-memory stream-group provisioning (``AckStreamGroupAdminPort``).

    Control-plane only: idempotent group creation over the same in-memory store as
    :class:`MockAckStreamGroupAdapter`. Kept separate from the data-plane query adapter so a
    read/ack/claim reference cannot reach group provisioning.
    """

    stream: MockStreamAdapter[M]
    state: MockState

    # ....................... #

    async def ensure_group(self, group: str, stream: str, *, start_id: str = "$") -> None:
        with self.state.lock:
            store = cast(dict[str, Any], self.stream._stream_store())  # type: ignore[reportPrivateUsage]
            groups = cast(
                dict[tuple[str, str], _MockGroupState],
                store.setdefault(_GROUPS_KEY, [{}])[0],
            )

            if (group, stream) in groups:
                return  # idempotent — already created

            gs = _MockGroupState()

            if start_id == "$":
                # "$": deliver only entries appended after creation (cursor at the tail)
                entries = cast(list[Any], store.setdefault(stream, []))
                gs.last_delivered = max(
                    (self.stream._id_to_int(m.id) for m in entries),  # type: ignore[reportPrivateUsage]
                    default=0,
                )
            elif start_id not in ("0", "0-0"):
                # an explicit cursor: deliver entries strictly after it ("0"/"0-0" stays at 0)
                gs.last_delivered = self.stream._id_to_int(start_id)  # type: ignore[reportPrivateUsage]

            groups[(group, stream)] = gs


# ....................... #


def _partition_for(selector: str, partitions: int) -> int:
    """Deterministically map a message's key (or id) to a partition.

    Uses a stable BLAKE2b digest — never Python's per-process-salted ``hash`` —
    so a message lands on the same partition across runs, keeping partition
    assignment reproducible under deterministic simulation.
    """

    if partitions <= 1:
        return 0

    digest = hashlib.blake2b(selector.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") % partitions


# ....................... #


def _assign_positions(
    log: Sequence[StreamMessage[Any]],
    partitions: int,
) -> list[tuple[StreamMessage[Any], int, int]]:
    """Derive ``(message, partition, offset)`` for a flat append-ordered log.

    The partition is chosen from the message's ``key`` (falling back to ``id``
    for keyless messages) and the offset is the per-partition running index, so
    same-key messages share a partition and keep their produce order — the
    offset-log's per-partition ordering guarantee.
    """

    counts: dict[int, int] = {}
    out: list[tuple[StreamMessage[Any], int, int]] = []

    for msg in log:
        selector = msg.key if msg.key is not None else msg.id
        partition = _partition_for(selector, partitions)
        offset = counts.get(partition, 0)
        counts[partition] = offset + 1
        out.append((msg, partition, offset))

    return out


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCommitStreamGroupAdapter[M: BaseModel](CommitStreamGroupQueryPort[M]):
    """In-memory offset-log consumer mirroring Kafka commit semantics.

    Reads the shared append-ordered log the producer (:class:`MockStreamAdapter`)
    writes to, deriving each message's ``(partition, offset)`` on the fly (so the
    unchanged producer path — including the outbox relay — feeds it directly).
    A per-``(group, topic, partition)`` committed cursor gates delivery: only
    :meth:`commit` advances it, so a read that is not committed is redelivered on
    the next read — modeling at-least-once on crash-before-commit. Partition
    assignment is by ``key`` hash, giving per-partition ordering for same-key
    messages. The group cursor is shared across a group's consumers (the mock
    does not simulate partition rebalancing).
    """

    stream: MockStreamAdapter[M]
    state: MockState
    namespace: str

    # ....................... #

    def _partitions(self, topic: str) -> int:
        return self.state.commit_stream_partitions.get(
            (self.stream._ns(), topic),  # type: ignore[reportPrivateUsage]
            1,
        )

    def _log(self, topic: str) -> list[StreamMessage[M]]:
        return self.stream._stream_store().setdefault(topic, [])  # type: ignore[reportPrivateUsage]

    def _cursor(self, group: str, topic: str, partition: int) -> int:
        return self.state.commit_stream_offsets.get(
            (self.stream._ns(), group, topic, partition),  # type: ignore[reportPrivateUsage]
            0,
        )

    # ....................... #

    async def read(
        self,
        group: str,
        consumer: str,
        topics: Sequence[str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[M]]:
        del consumer, timeout
        out: list[StreamMessage[M]] = []

        with self.state.lock:
            for topic in topics:
                partitions = self._partitions(topic)

                for msg, partition, offset in _assign_positions(
                    self._log(topic), partitions
                ):
                    if offset < self._cursor(group, topic, partition):
                        continue

                    out.append(
                        attrs.evolve(
                            msg,
                            partition=partition,
                            offset=offset,
                            id=f"{topic}:{partition}:{offset}",
                        )
                    )

                    if limit is not None and len(out) >= limit:
                        return out

        return out

    # ....................... #

    async def tail(
        self,
        group: str,
        consumer: str,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        while True:
            messages = await self.read(group, consumer, topics, timeout=timeout)

            for message in messages:
                yield message

            if not messages:
                await asyncio.sleep(_sleep_interval(timeout))

    # ....................... #

    async def commit(self, group: str, positions: Sequence[StreamPosition]) -> None:
        with self.state.lock:
            for pos in positions:
                key = (self.stream._ns(), group, pos.stream, pos.partition)  # type: ignore[reportPrivateUsage]
                nxt = pos.offset + 1

                if nxt > self.state.commit_stream_offsets.get(key, 0):
                    self.state.commit_stream_offsets[key] = nxt

    # ....................... #

    async def seek_to_committed(self, group: str, topics: Sequence[str]) -> None:
        # No-op: the mock has no in-memory read position — every ``read`` re-derives
        # delivery from the committed cursor, so an uncommitted batch is already
        # re-fetched on the next read. Present only to satisfy the port contract.
        del group, topics

    # ....................... #

    def capabilities(self) -> CommitStreamGroupCapabilities:
        return CommitStreamGroupCapabilities(
            supports_replay=True,
            supports_transactions=False,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockCommitStreamGroupAdminAdapter[M: BaseModel](CommitStreamGroupAdminPort):
    """In-memory offset-log control plane (``CommitStreamGroupAdminPort``).

    Topic provisioning (partition count), first-time group positioning, replay
    (``reset_offsets``), and per-partition lag over the same shared log and
    cursor store as :class:`MockCommitStreamGroupAdapter`. Kept separate from the
    data-plane consumer so a read/commit reference cannot reach provisioning or
    replay.
    """

    stream: MockStreamAdapter[M]
    state: MockState

    # ....................... #

    def _ns(self) -> str:
        return self.stream._ns()  # type: ignore[reportPrivateUsage]

    def _partitions(self, topic: str) -> int:
        return self.state.commit_stream_partitions.get((self._ns(), topic), 1)

    def _log(self, topic: str) -> list[StreamMessage[Any]]:
        return self.stream._stream_store().setdefault(topic, [])  # type: ignore[reportPrivateUsage]

    def _end_offsets(self, topic: str) -> dict[int, int]:
        partitions = self._partitions(topic)
        counts: dict[int, int] = {p: 0 for p in range(partitions)}

        for _msg, partition, _offset in _assign_positions(
            self._log(topic), partitions
        ):
            counts[partition] = counts.get(partition, 0) + 1

        return counts

    def _all_topics(self) -> list[str]:
        return [k for k in self.stream._stream_store() if k != _GROUPS_KEY]  # type: ignore[reportPrivateUsage]

    def _offset_at_timestamp(
        self, when: datetime | None, *, end: int, topic: str, partition: int
    ) -> int:
        """First offset on *partition* whose message timestamp is at or after *when* (else *end*)."""

        for msg, msg_partition, offset in _assign_positions(
            self._log(topic), self._partitions(topic)
        ):
            if msg_partition != partition:
                continue

            if when is not None and msg.timestamp is not None and msg.timestamp >= when:
                return offset

        return end

    def _target_offset(
        self, target: OffsetReset, *, end: int, topic: str, partition: int
    ) -> int:
        if target.kind is OffsetResetKind.EARLIEST:
            return 0

        if target.kind is OffsetResetKind.LATEST:
            return end

        if target.kind is OffsetResetKind.OFFSET:
            offset = target.offset if target.offset is not None else 0
            return max(0, min(offset, end))

        return self._offset_at_timestamp(
            target.timestamp, end=end, topic=topic, partition=partition
        )

    # ....................... #

    async def ensure_topic(
        self,
        stream: str,
        *,
        partitions: int,
        replication: int = 1,
        config: Mapping[str, str] | None = None,
    ) -> None:
        del replication, config

        if partitions < 1:
            raise exc.validation("ensure_topic requires partitions >= 1")

        with self.state.lock:
            key = (self._ns(), stream)
            existing = self.state.commit_stream_partitions.get(key)

            if existing is not None:
                if existing != partitions:
                    raise exc.configuration(
                        f"Stream {stream!r} already exists with {existing} partitions; "
                        f"cannot ensure it with {partitions} partitions."
                    )
                return

            self.state.commit_stream_partitions[key] = partitions

    # ....................... #

    async def ensure_group(
        self,
        group: str,
        topics: Sequence[str],
        *,
        start: OffsetReset = OffsetReset.LATEST,
    ) -> None:
        with self.state.lock:
            for topic in topics:
                partitions = self._partitions(topic)
                ends = self._end_offsets(topic)

                for partition in range(partitions):
                    key = (self._ns(), group, topic, partition)

                    if key in self.state.commit_stream_offsets:
                        continue  # first-time positioning only

                    self.state.commit_stream_offsets[key] = self._target_offset(
                        start, end=ends.get(partition, 0), topic=topic, partition=partition
                    )

    # ....................... #

    async def reset_offsets(
        self, group: str, stream: str, *, to: OffsetReset
    ) -> None:
        if not self.capabilities().supports_replay:
            raise exc.configuration(
                f"Stream {stream!r} backend does not support offset reset / replay.",
                code="stream.replay_unsupported",
            )

        with self.state.lock:
            partitions = self._partitions(stream)
            ends = self._end_offsets(stream)

            for partition in range(partitions):
                self.state.commit_stream_offsets[
                    (self._ns(), group, stream, partition)
                ] = self._target_offset(
                    to, end=ends.get(partition, 0), topic=stream, partition=partition
                )

    # ....................... #

    async def lag(self, group: str, stream: str | None = None) -> list[ConsumerLag]:
        out: list[ConsumerLag] = []

        with self.state.lock:
            topics = [stream] if stream is not None else self._all_topics()

            for topic in topics:
                ends = self._end_offsets(topic)

                for partition in range(self._partitions(topic)):
                    committed = self.state.commit_stream_offsets.get(
                        (self._ns(), group, topic, partition), 0
                    )
                    end = ends.get(partition, 0)
                    out.append(
                        ConsumerLag(
                            stream=topic,
                            partition=partition,
                            committed_offset=committed,
                            end_offset=end,
                        )
                    )

        return out

    # ....................... #

    def capabilities(self) -> CommitStreamGroupCapabilities:
        return CommitStreamGroupCapabilities(
            supports_replay=True,
            supports_transactions=False,
        )
