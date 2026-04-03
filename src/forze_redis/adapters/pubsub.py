"""Redis-backed pub/sub adapters implementing publish and subscribe ports."""

from forze.infra.tenancy import MultiTenancyMixin
from forze_redis._compat import require_redis

require_redis()

# ....................... #

from datetime import datetime, timedelta
from typing import AsyncIterator, Sequence, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.pubsub import (
    PubSubMessage,
    PubSubPublishPort,
    PubSubSubscribePort,
)

from ..kernel.platform import RedisClient
from .codecs import RedisPubSubCodec

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RedisPubSubAdapter[M: BaseModel](
    PubSubPublishPort[M], PubSubSubscribePort[M], MultiTenancyMixin
):
    """Redis implementation of :class:`~forze.application.contracts.pubsub.PubSubPublishPort` and :class:`~forze.application.contracts.pubsub.PubSubSubscribePort`.

    Publishes JSON-encoded messages via ``PUBLISH`` and yields decoded
    :class:`~forze.application.contracts.pubsub.PubSubMessage` instances by
    subscribing to Redis channels through :class:`RedisClient`.
    """

    client: RedisClient
    """Redis client instance."""

    codec: RedisPubSubCodec[M]
    """PubSub codec instance - used for encoding and decoding messages."""

    # ....................... #

    def __topic(self, topic: str) -> str:
        tenant_id = self.require_tenant_if_aware()

        #! maybe use redis key codec instead ...
        if tenant_id is not None:
            return f"tenant:{tenant_id}:pubsub:{topic}"

        return topic

    # ....................... #

    async def publish(
        self,
        topic: str,
        payload: M,
        *,
        type: str | None = None,
        key: str | None = None,
        published_at: datetime | None = None,
    ) -> None:
        data = self.codec.encode(
            payload,
            type=type,
            key=key,
            published_at=published_at,
        )
        topic = self.__topic(topic)

        await self.client.publish(topic, data)

    # ....................... #

    async def subscribe(
        self,
        topics: Sequence[str],
        *,
        timeout: timedelta | None = None,
    ) -> AsyncIterator[PubSubMessage[M]]:
        topics = list(map(self.__topic, topics))

        async for topic, raw_data in self.client.subscribe(topics, timeout=timeout):
            yield self.codec.decode(topic, raw_data)
