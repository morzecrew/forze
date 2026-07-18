"""Redis Streams adapters implementing read, write, and consumer-group ports."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

import asyncio
from collections.abc import AsyncGenerator, Mapping, Sequence
from datetime import datetime, timedelta
from typing import cast, final

import attrs
from pydantic import BaseModel
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from forze.application.contracts.stream import (
    AckGroupDepth,
    AckStreamGroupAdminPort,
    AckStreamGroupQueryPort,
    PendingEntry,
    StreamCommandPort,
    StreamMessage,
    StreamQueryPort,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.exceptions import exc

from ..kernel.client import RedisClientPort
from .codecs import RedisStreamCodec

# ----------------------- #

_STREAM_READ_RETRY_EXC: tuple[type[BaseException], ...] = (
    RedisConnectionError,
    RedisTimeoutError,
    TimeoutError,
    OSError,
)

_AUTOCLAIM_EXHAUSTED: str = "0-0"
"""``XAUTOCLAIM`` cursor value signalling the pending list was fully scanned."""

_PENDING_PAGE_SIZE: int = 100
"""``XPENDING`` page size used when walking an unbounded pending list."""


def _stream_wire_and_back(
    mixin: TenancyMixin,
    stream_mapping: dict[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Map logical stream names to tenant-prefixed Redis keys and build reverse map."""

    wired: dict[str, str] = {}
    back: dict[str, str] = {}

    tenant_id = mixin.require_tenant_if_aware()

    for logical, cursor in stream_mapping.items():
        physical = f"tenant:{tenant_id}:stream:{logical}" if tenant_id is not None else logical

        wired[physical] = cursor
        back[physical] = logical

    return wired, back


def _stream_physical(mixin: TenancyMixin, stream: str) -> str:
    tenant_id = mixin.require_tenant_if_aware()
    if tenant_id is not None:
        return f"tenant:{tenant_id}:stream:{stream}"
    return stream


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamAdapter[M: BaseModel](
    StreamQueryPort[M],
    StreamCommandPort[M],
    TenancyMixin,
):
    """Redis implementation of :class:`~forze.application.contracts.stream.StreamQueryPort` and :class:`~forze.application.contracts.stream.StreamCommandPort`.

    Reads via ``XREAD`` and appends via ``XADD``.  :meth:`tail` polls
    continuously, advancing the per-stream cursor after each message.
    When ``tenant_aware`` is true, stream names are isolated per tenant using
    the same ``tenant:{id}:stream:`` prefix pattern as :class:`RedisPubSubAdapter`.
    """

    client: RedisClientPort
    codec: RedisStreamCodec[M]

    max_entries: int | None = None
    """Approximate retention cap applied at every append (``XADD MAXLEN ~``); ``None`` = no
    cap. See ``RedisStreamConfig.retention_max_entries`` for sizing guidance."""

    # ....................... #

    async def read(
        self,
        stream_mapping: dict[str, str],
        *,
        limit: int | None = None,
        timeout: timedelta | None = None,
    ) -> list[StreamMessage[M]]:
        wired, back = _stream_wire_and_back(self, stream_mapping)

        raw = await self.client.xread(
            wired,
            count=limit,
            block_ms=int(timeout.total_seconds() * 1000) if timeout else None,
        )

        out: list[StreamMessage[M]] = []

        for stream, entries in raw:
            logical = back.get(stream, stream)
            for msg_id, fields in entries:
                out.append(self.codec.decode(logical, msg_id, fields))

        return out

    # ....................... #

    async def tail(
        self,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncGenerator[StreamMessage[M]]:
        cursor = dict(stream_mapping)
        backoff = 0.05
        max_backoff = 30.0

        while True:
            try:
                messages = await self.read(cursor, timeout=timeout)

            except _STREAM_READ_RETRY_EXC:
                await asyncio.sleep(min(max_backoff, backoff))
                backoff = min(max_backoff, backoff * 2)
                continue

            backoff = 0.05

            if not messages and timeout is None:
                await asyncio.sleep(0)

            for m in messages:
                cursor[m.stream] = m.id
                yield m

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
        data = self.codec.encode(
            payload,
            type=type,
            key=key,
            timestamp=timestamp,
            headers=headers,
        )

        return await self.client.xadd(_stream_physical(self, stream), data, maxlen=self.max_entries)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamGroupAdapter[M: BaseModel](AckStreamGroupQueryPort[M], TenancyMixin):
    """Redis implementation of :class:`~forze.application.contracts.stream.AckStreamGroupQueryPort`.

    Reads via ``XREADGROUP`` with ``noack=False`` so messages enter the pending
    list until :meth:`ack` is called.  :meth:`tail` polls continuously,
    advancing the per-stream cursor after each message.  Pending-entry recovery
    maps to the native commands: :meth:`claim` sweeps the PEL with
    ``XAUTOCLAIM`` (looping the scan cursor) and :meth:`pending` pages the
    extended ``XPENDING`` form.
    """

    client: RedisClientPort
    codec: RedisStreamCodec[M]

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
        wired, back = _stream_wire_and_back(self, stream_mapping)

        raw = await self.client.xgroup_read(
            group=group,
            consumer=consumer,
            streams=wired,
            count=limit,
            block_ms=int(timeout.total_seconds() * 1000) if timeout else None,
            noack=False,
        )

        out: list[StreamMessage[M]] = []

        for stream, entries in raw:
            logical = back.get(stream, stream)
            for msg_id, fields in entries:
                out.append(self.codec.decode(logical, msg_id, fields))

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
        backoff = 0.05
        max_backoff = 30.0

        while True:
            try:
                messages = await self.read(group, consumer, cursor, timeout=timeout)

            except _STREAM_READ_RETRY_EXC:
                await asyncio.sleep(min(max_backoff, backoff))
                backoff = min(max_backoff, backoff * 2)
                continue

            backoff = 0.05

            if not messages and timeout is None:
                await asyncio.sleep(0)

            for m in messages:
                cursor[m.stream] = m.id
                yield m

    # ....................... #

    async def ack(self, group: str, stream: str, ids: Sequence[str]) -> int:
        return await self.client.xack(_stream_physical(self, stream), group, ids)

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
        physical = _stream_physical(self, stream)
        min_idle_ms = int(idle.total_seconds() * 1000)

        out: list[StreamMessage[M]] = []
        cursor = _AUTOCLAIM_EXHAUSTED

        while True:
            remaining = None if limit is None else limit - len(out)

            if remaining is not None and remaining <= 0:
                break

            # One XAUTOCLAIM page; ``count=None`` lets the server default
            # (100) drive unbounded sweeps, page by page.
            next_cursor, entries, _deleted = await self.client.xautoclaim(
                physical,
                group,
                consumer,
                min_idle_ms=min_idle_ms,
                start_id=cursor,
                count=remaining,
            )

            for msg_id, fields in entries:
                out.append(self.codec.decode(stream, msg_id, fields))

            if next_cursor == _AUTOCLAIM_EXHAUSTED:
                break

            cursor = next_cursor

        return out

    # ....................... #

    async def pending(
        self,
        group: str,
        stream: str,
        *,
        limit: int | None = None,
    ) -> list[PendingEntry]:
        physical = _stream_physical(self, stream)

        out: list[PendingEntry] = []
        cursor = "-"

        while True:
            remaining = None if limit is None else limit - len(out)

            if remaining is not None and remaining <= 0:
                break

            count = _PENDING_PAGE_SIZE if remaining is None else min(_PENDING_PAGE_SIZE, remaining)

            rows = await self.client.xpending(
                physical,
                group,
                count=count,
                start_id=cursor,
            )

            for msg_id, owner, idle_ms, delivered in rows:
                out.append(
                    PendingEntry(
                        id=msg_id,
                        consumer=owner,
                        idle=timedelta(milliseconds=idle_ms),
                        delivery_count=delivered,
                    )
                )

            if len(rows) < count:
                break

            # Advance past the last seen id (exclusive range cursor).
            cursor = f"({rows[-1][0]}"

        return out


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamGroupAdminAdapter(AckStreamGroupAdminPort, TenancyMixin):
    """Redis implementation of :class:`~forze.application.contracts.stream.AckStreamGroupAdminPort`.

    Control-plane only: idempotent consumer-group creation (``XGROUP CREATE …
    MKSTREAM``). Kept separate from the data-plane :class:`RedisStreamGroupAdapter`
    so a read/ack/claim reference cannot reach group provisioning.
    """

    client: RedisClientPort

    # ....................... #

    async def ensure_group(self, group: str, stream: str, *, start_id: str = "$") -> None:
        try:
            await self.client.xgroup_create(
                _stream_physical(self, stream), group, id=start_id, mkstream=True
            )

        except Exception as error:
            if "BUSYGROUP" not in str(error):
                raise

    # ....................... #

    async def depth(self, group: str, stream: str) -> AckGroupDepth:
        physical = _stream_physical(self, stream)

        entry: dict[str, object] | None = None
        for row in await self.client.xinfo_groups(physical):
            name = row.get("name")
            name = name.decode() if isinstance(name, bytes) else name
            if name == group:
                entry = row
                break

        if entry is None:
            raise exc.not_found(
                f"Stream group {group!r} does not exist on {stream!r} — run the "
                "ensure-group step before observing depth",
                code="stream_group_not_found",
            )

        # Redis ≥7 reports lag directly; None after trims/interior deletions (kept as
        # unknown — AckGroupDepth treats an unknown backlog as not-at-rest).
        raw_lag = entry.get("lag")
        backlog = int(raw_lag) if isinstance(raw_lag, int) else None
        pending_count = int(cast(int, entry.get("pending", 0)))

        oldest: timedelta | None = None
        if pending_count:
            oldest_rows = await self.client.xpending(physical, group, count=1)
            if oldest_rows:
                _msg_id, _owner, idle_ms, _delivered = oldest_rows[0]
                oldest = timedelta(milliseconds=idle_ms)

        return AckGroupDepth(
            backlog=backlog,
            pending=pending_count,
            oldest_pending_idle=oldest,
        )
