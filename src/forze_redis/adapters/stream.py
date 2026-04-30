"""Redis Streams adapters implementing read, write, and consumer-group ports."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import datetime, timedelta
from typing import AsyncIterator, Sequence, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.stream import (
    StreamCommandPort,
    StreamGroupQueryPort,
    StreamMessage,
    StreamQueryPort,
)

from ..kernel.platform import RedisClientPort
from .codecs import RedisStreamCodec

# ----------------------- #
#! TODO: add multi-tenancy support


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamAdapter[M: BaseModel](StreamQueryPort[M], StreamCommandPort[M]):
    """Redis implementation of :class:`~forze.application.contracts.stream.StreamQueryPort` and :class:`~forze.application.contracts.stream.StreamCommandPort`.

    Reads via ``XREAD`` and appends via ``XADD``.  :meth:`tail` polls
    continuously, advancing the per-stream cursor after each message.
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
        raw = await self.client.xread(
            stream_mapping,
            count=limit,
            block_ms=int(timeout.total_seconds() * 1000) if timeout else None,
        )

        out: list[StreamMessage[M]] = []

        for stream, entries in raw:
            for msg_id, fields in entries:
                out.append(self.codec.decode(stream, msg_id, fields))

        return out

    # ....................... #

    async def tail(
        self,
        stream_mapping: dict[str, str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[StreamMessage[M]]:
        cursor = dict(stream_mapping)

        while True:
            messages = await self.read(cursor, timeout=timeout)

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

        return await self.client.xadd(stream, data)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamGroupAdapter[M: BaseModel](StreamGroupQueryPort[M]):
    """Redis implementation of :class:`~forze.application.contracts.stream.StreamGroupQueryPort`.

    Reads via ``XREADGROUP`` with ``noack=True`` and acknowledges messages
    explicitly through :meth:`ack`.  :meth:`tail` polls continuously,
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
        raw = await self.client.xgroup_read(
            group=group,
            consumer=consumer,
            streams=stream_mapping,
            count=limit,
            block_ms=int(timeout.total_seconds() * 1000) if timeout else None,
            noack=True,
        )

        out: list[StreamMessage[M]] = []

        for stream, entries in raw:
            for msg_id, fields in entries:
                out.append(self.codec.decode(stream, msg_id, fields))

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

        while True:
            messages = await self.read(group, consumer, cursor, timeout=timeout)

            for m in messages:
                cursor[m["stream"]] = m["id"]
                yield m

    # ....................... #

    async def ack(self, group: str, stream: str, ids: Sequence[str]) -> int:
        return await self.client.xack(group=group, stream=stream, ids=ids)
