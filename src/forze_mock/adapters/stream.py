"""In-memory stream adapters."""

from __future__ import annotations

import asyncio
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
    PendingEntry,
    StreamCommandPort,
    StreamGroupQueryPort,
    StreamMessage,
    StreamQueryPort,
)
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
class MockStreamGroupAdapter[M: BaseModel](StreamGroupQueryPort[M]):
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
