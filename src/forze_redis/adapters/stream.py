from forze_redis._compat import require_redis

require_redis()

# ....................... #

from typing import (
    AsyncIterator,
    Final,
    NotRequired,
    Optional,
    Sequence,
    TypedDict,
    final,
)

import attrs
from pydantic import BaseModel

from forze.application.contracts.stream import StreamEvent, StreamPort
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_dump, pydantic_validate
from forze.utils.codecs import JsonCodec, TextCodec

from ..kernel.platform import RedisClient

# ----------------------- #
#! TODO: add tenant context support

# make var names fully private ("__" prefix)
PAYLOAD_KEY: Final[str] = "data"
TYPE_KEY: Final[str] = "t"
TS_KEY: Final[str] = "ts"
KEY_KEY: Final[str] = "k"

# ....................... #


@final
class _Payload(TypedDict):
    """Payload as sent to Redis (data is serialized bytes)."""

    data: bytes
    t: NotRequired[str]
    ts: NotRequired[int]
    k: NotRequired[str]


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisStreamAdapter[M: BaseModel](StreamPort[M]):
    client: RedisClient
    model: type[M]

    # Non initable fields
    json_codec: JsonCodec = attrs.field(factory=JsonCodec, init=False)
    text_codec: TextCodec = attrs.field(factory=TextCodec, init=False)

    # Defaults (overrideable)
    maxlen: int = 2000
    approx: bool = True

    # ....................... #

    async def publish(
        self,
        stream: str,
        payload: M | JsonDict,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        ts: Optional[int] = None,
        id: str = "*",
        maxlen: Optional[int] = None,
        approx: Optional[bool] = None,
    ) -> str:
        if isinstance(payload, BaseModel):
            payload = pydantic_dump(payload)

        sp = _Payload(data=self.json_codec.dumps(payload))

        if type is not None:
            sp[TYPE_KEY] = type

        if ts is not None:
            sp[TS_KEY] = ts

        if key is not None:
            sp[KEY_KEY] = key

        use_maxlen = maxlen or self.maxlen
        use_approx = approx or self.approx

        return await self.client.xadd(
            stream,
            dict(sp),
            id=id,
            maxlen=use_maxlen,
            approx=use_approx,
        )

    # ....................... #

    def __raw_to_events(
        self,
        raw: list[tuple[str, list[tuple[str, dict[bytes, bytes]]]]],
    ) -> list[StreamEvent[M]]:
        events: list[StreamEvent[M]] = []

        key_data = self.text_codec.dumps(PAYLOAD_KEY)
        key_t = self.text_codec.dumps(TYPE_KEY)
        key_ts = self.text_codec.dumps(TS_KEY)
        key_k = self.text_codec.dumps(KEY_KEY)

        for s, msgs in raw:
            for msg_id, data in msgs:
                if key_data not in data:
                    continue

                decoded = self.json_codec.loads(data[key_data])
                data_model = pydantic_validate(self.model, decoded)
                type_ = self.text_codec.loads(data[key_t]) if key_t in data else None
                ts = (
                    int(self.text_codec.loads(data[key_ts])) if key_ts in data else None
                )
                k = self.text_codec.loads(data[key_k]) if key_k in data else None

                events.append(
                    StreamEvent(
                        stream=s,
                        id=msg_id,
                        type=type_,
                        timestamp=ts,
                        key=k,
                        data=data_model,
                    )
                )

        return events

    # ....................... #

    async def read(
        self,
        streams: dict[str, str],
        *,
        count: Optional[int] = None,
        block_ms: Optional[int] = None,  #! use timedelta
    ) -> list[StreamEvent[M]]:
        res = await self.client.xread(streams, count=count, block_ms=block_ms)

        return self.__raw_to_events(res)

    # ....................... #

    async def subscribe(
        self,
        stream: str,
        *,
        start_id: str = "$",
        block_ms: int = 5000,  #! use timedelta
        count: int = 200,
    ) -> AsyncIterator[StreamEvent[M]]:
        last_id = start_id

        while True:
            events = await self.read(
                streams={stream: last_id},
                count=count,
                block_ms=block_ms,
            )

            if not events:
                continue

            for ev in events:
                yield ev

                last_id = ev["id"]

    # ....................... #

    async def trim(
        self,
        stream: str,
        *,
        maxlen: int,
        approx: bool = True,
        limit: Optional[int] = None,
    ) -> int:
        return await self.client.xtrim_maxlen(
            stream,
            maxlen,
            approx=approx,
            limit=limit,
        )

    # ....................... #

    async def delete(self, stream: str, ids: Sequence[str]) -> int:
        return await self.client.xdel(stream, ids)

    # ....................... #

    async def ensure_group(
        self,
        stream: str,
        group: str,
        *,
        start_id: str = "0-0",
        mkstream: bool = True,
        ignore_busy: bool = True,
    ) -> bool:
        try:
            return await self.client.xgroup_create(
                stream, group, id=start_id, mkstream=mkstream
            )

        except Exception as e:
            msg = str(e)

            if ignore_busy and "busy" in msg.lower():
                return False

            raise

    # ....................... #

    async def read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        start_id: str = ">",
        block_ms: Optional[int] = None,  #! use timedelta
        count: Optional[int] = None,
        noack: bool = False,
    ) -> list[StreamEvent[M]]:
        res = await self.client.xgroup_read(
            group,
            consumer,
            streams={stream: start_id},
            count=count,
            block_ms=block_ms,
            noack=noack,
        )

        return self.__raw_to_events(res)

    # ....................... #

    def subscribe_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        start_id: str = ">",
        block_ms: int = 5000,  #! use timedelta
        count: int = 200,
    ) -> AsyncIterator[StreamEvent[M]]:
        raise NotImplementedError("Not implemented")

    # ....................... #

    async def ack(self, stream: str, group: str, ids: Sequence[str]) -> int:
        return await self.client.xack(stream, group, ids)
