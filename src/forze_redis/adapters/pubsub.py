"""Redis-backed pub/sub adapters implementing publish and subscribe ports."""

from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import datetime, timedelta
from typing import AsyncIterator, Final, Optional, Sequence, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.pubsub import (
    PubSubMessage,
    PubSubPublishPort,
    PubSubSubscribePort,
)
from forze.base.codecs import JsonCodec
from forze.base.errors import CoreError
from forze.base.logging_v2 import getLogger

from ..kernel.platform import RedisClient

# ----------------------- #

logger = getLogger(__name__).bind(scope="redis.pubsub")

# ....................... #

_F_PAYLOAD: Final[str] = "payload"
_F_TYPE: Final[str] = "type"
_F_PUBLISHED_AT: Final[str] = "published_at"
_F_KEY: Final[str] = "key"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisPubSubCodec[M: BaseModel]:
    """JSON codec that serialises and deserialises :class:`~forze.application.contracts.pubsub.PubSubMessage` payloads.

    :meth:`encode` wraps a Pydantic model into a JSON envelope with optional
    metadata fields (``type``, ``key``, ``published_at``).  :meth:`decode`
    reconstructs the :class:`~forze.application.contracts.pubsub.PubSubMessage`
    from the raw channel bytes.
    """

    model: type[M]
    json_codec: JsonCodec = attrs.field(factory=JsonCodec)

    # ....................... #

    def encode(
        self,
        payload: M,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        published_at: Optional[datetime] = None,
    ) -> bytes:
        data: dict[str, str] = {_F_PAYLOAD: payload.model_dump_json()}

        if type is not None:
            data[_F_TYPE] = type

        if key is not None:
            data[_F_KEY] = key

        if published_at is not None:
            data[_F_PUBLISHED_AT] = published_at.isoformat()

        return self.json_codec.dumps(data)

    # ....................... #

    def decode(self, topic: str, raw_data: bytes | str) -> PubSubMessage[M]:
        decoded = self.json_codec.loads(raw_data)

        if not isinstance(decoded, dict):
            raise CoreError(f"Redis pubsub message in '{topic}' has invalid payload")

        payload_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_PAYLOAD
        )

        if not isinstance(payload_raw, str):
            raise CoreError(f"Redis pubsub message in '{topic}' has no payload")

        type_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_TYPE
        )
        key_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_KEY
        )
        published_at_raw = decoded.get(  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
            _F_PUBLISHED_AT
        )

        return PubSubMessage(
            topic=topic,
            payload=self.model.model_validate_json(payload_raw),
            type=type_raw if isinstance(type_raw, str) else None,
            key=key_raw if isinstance(key_raw, str) else None,
            published_at=(
                datetime.fromisoformat(published_at_raw)
                if isinstance(published_at_raw, str)
                else None
            ),
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisPubSubAdapter[M: BaseModel](PubSubPublishPort[M], PubSubSubscribePort[M]):
    """Redis implementation of :class:`~forze.application.contracts.pubsub.PubSubPublishPort` and :class:`~forze.application.contracts.pubsub.PubSubSubscribePort`.

    Publishes JSON-encoded messages via ``PUBLISH`` and yields decoded
    :class:`~forze.application.contracts.pubsub.PubSubMessage` instances by
    subscribing to Redis channels through :class:`RedisClient`.
    """

    client: RedisClient
    codec: RedisPubSubCodec[M]

    # ....................... #

    async def publish(
        self,
        topic: str,
        payload: M,
        *,
        type: Optional[str] = None,
        key: Optional[str] = None,
        published_at: Optional[datetime] = None,
    ) -> None:
        data = self.codec.encode(
            payload,
            type=type,
            key=key,
            published_at=published_at,
        )
        await self.client.publish(topic, data)

    # ....................... #

    async def subscribe(
        self,
        topics: Sequence[str],
        *,
        timeout: Optional[timedelta] = None,
    ) -> AsyncIterator[PubSubMessage[M]]:
        async for topic, raw_data in self.client.subscribe(topics, timeout=timeout):
            yield self.codec.decode(topic, raw_data)
