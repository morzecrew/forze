"""Redis Streams adapters implementing read, write, and consumer-group ports."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

import asyncio
from datetime import datetime, timedelta
from typing import AsyncIterator, Sequence, final

import attrs
from pydantic import BaseModel
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from forze.application.contracts.stream import (
    StreamCommandPort,
    StreamGroupQueryPort,
    StreamMessage,
    StreamQueryPort,
)
from forze.application.contracts.tenancy import TenancyMixin

from ..kernel.platform import RedisClientPort
from .codecs import RedisStreamCodec

# ----------------------- #

_STREAM_READ_RETRY_EXC: tuple[type[BaseException], ...] = (
    RedisConnectionError,
    RedisTimeoutError,
    TimeoutError,
    OSError,
)


def _stream_wire_and_back(
    mixin: TenancyMixin,
    stream_mapping: dict[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Map logical stream names to tenant-prefixed Redis keys and build reverse map."""

    wired: dict[str, str] = {}
    back: dict[str, str] = {}

    tenant_id = mixin.require_tenant_if_aware()

    for logical, cursor in stream_mapping.items():
        if tenant_id is not None:
            physical = f"tenant:{tenant_id}:stream:{logical}"
        else:
            physical = logical

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
    ) -> AsyncIterator[StreamMessage[M]]:
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
                cursor[m["stream"]] = m["id"]
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
    ) -> str:
        data = self.codec.encode(payload, type=type, key=key, timestamp=timestamp)

        return await self.client.xadd(_stream_physical(self, stream), data)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamGroupAdapter[M: BaseModel](StreamGroupQueryPort[M], TenancyMixin):
    """Redis implementation of :class:`~forze.application.contracts.stream.StreamGroupQueryPort`.

    Reads via ``XREADGROUP`` with ``noack=False`` so messages enter the pending
    list until :meth:`ack` is called.  :meth:`tail` polls continuously,
    advancing the per-stream cursor after each message.
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
    ) -> AsyncIterator[StreamMessage[M]]:
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
                cursor[m["stream"]] = m["id"]
                yield m

    # ....................... #

    async def ack(self, group: str, stream: str, ids: Sequence[str]) -> int:
        return await self.client.xack(_stream_physical(self, stream), group, ids)
