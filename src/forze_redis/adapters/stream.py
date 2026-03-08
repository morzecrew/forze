from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import datetime, timedelta
from typing import AsyncIterator, Final, Optional, Sequence, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.stream import (
    StreamGroupPort,
    StreamMessage,
    StreamReadPort,
    StreamWritePort,
)
from forze.base.errors import CoreError

from ..kernel.platform import RedisClient

# ----------------------- #

_F_PAYLOAD: Final[str] = "payload"
_F_TYPE: Final[str] = "type"
_F_TIMESTAMP: Final[str] = "timestamp"
_F_KEY: Final[str] = "key"

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamCodec[M: BaseModel]:
    model: type[M]

    # ....................... #

    def encode(
        self,
        payload: M,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> dict[str, str]:
        data: dict[str, str] = {_F_PAYLOAD: payload.model_dump_json()}

        if type is not None:
            data[_F_TYPE] = type

        if key is not None:
            data[_F_KEY] = key

        if timestamp is not None:
            data[_F_TIMESTAMP] = timestamp.isoformat()

        return data

    # ....................... #

    def decode(self, stream: str, id: str, raw_data: dict[bytes, bytes]):
        decoded = {k.decode("utf-8"): v.decode("utf-8") for k, v in raw_data.items()}
        payload_raw = decoded.get(_F_PAYLOAD)

        if payload_raw is None:
            raise CoreError(f"Redis stream message '{id}' in '{stream}' has no payload")

        timestamp_raw = decoded.get(_F_TIMESTAMP)

        return StreamMessage(
            stream=stream,
            id=id,
            payload=self.model.model_validate_json(payload_raw),
            type=decoded.get(_F_TYPE),
            key=decoded.get(_F_KEY),
            timestamp=datetime.fromisoformat(timestamp_raw) if timestamp_raw else None,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamAdapter[M: BaseModel](StreamReadPort[M], StreamWritePort[M]):
    client: RedisClient
    codec: RedisStreamCodec[M]

    # ....................... #

    async def read(
        self,
        stream_mapping: dict[str, str],
        *,
        limit: Optional[int] = None,
        timeout: Optional[timedelta] = None,
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
        timeout: Optional[timedelta] = None,
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
        type: Optional[str] = None,
        key: Optional[str] = None,
        timestamp: Optional[datetime] = None,
    ) -> str:
        data = self.codec.encode(payload, type=type, key=key, timestamp=timestamp)

        return await self.client.xadd(stream, data)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamGroupAdapter[M: BaseModel](StreamGroupPort[M]):
    client: RedisClient
    codec: RedisStreamCodec[M]

    # ....................... #

    async def read(
        self,
        group: str,
        consumer: str,
        stream_mapping: dict[str, str],
        *,
        limit: Optional[int] = None,
        timeout: Optional[timedelta] = None,
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
        timeout: Optional[timedelta] = None,
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
