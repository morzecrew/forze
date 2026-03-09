from datetime import datetime, timedelta
from typing import Optional
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze_redis.adapters.pubsub import RedisPubSubAdapter, RedisPubSubCodec
from forze_redis.kernel.platform.client import RedisClient


class _Payload(BaseModel):
    value: str


def test_pubsub_codec_encode_decode_roundtrip() -> None:
    codec = RedisPubSubCodec(model=_Payload)
    payload = _Payload(value="hello")
    now = datetime(2025, 1, 1, 12, 0, 0)

    encoded = codec.encode(
        payload,
        type="created",
        key="partition-a",
        published_at=now,
    )
    decoded = codec.decode("orders", encoded)

    assert decoded["topic"] == "orders"
    assert decoded["payload"].value == "hello"
    assert decoded["type"] == "created"
    assert decoded["key"] == "partition-a"
    assert decoded["published_at"] == now


def test_pubsub_codec_decode_without_payload_raises() -> None:
    codec = RedisPubSubCodec(model=_Payload)

    with pytest.raises(CoreError, match="has no payload"):
        codec.decode("orders", b'{"type":"created"}')


@pytest.mark.asyncio
async def test_pubsub_adapter_publish_calls_client_publish() -> None:
    client = Mock(spec=RedisClient)
    client.publish = AsyncMock(return_value=1)
    adapter = RedisPubSubAdapter(client=client, codec=RedisPubSubCodec(model=_Payload))

    await adapter.publish("orders", _Payload(value="hello"))

    client.publish.assert_awaited_once()


@pytest.mark.asyncio
async def test_pubsub_adapter_subscribe_decodes_messages() -> None:
    client = Mock(spec=RedisClient)
    codec = RedisPubSubCodec(model=_Payload)
    captured: dict[str, object] = {}

    async def _iter():
        yield ("orders", codec.encode(_Payload(value="hello")))

    def _subscribe(topics, timeout: Optional[timedelta] = None):
        captured["topics"] = topics
        captured["timeout"] = timeout
        return _iter()

    client.subscribe = Mock(side_effect=_subscribe)
    adapter = RedisPubSubAdapter(client=client, codec=codec)

    stream = adapter.subscribe(["orders"])
    msg = await anext(stream)
    await stream.aclose()

    assert captured["topics"] == ["orders"]
    assert captured["timeout"] is None
    assert msg["topic"] == "orders"
    assert msg["payload"].value == "hello"


@pytest.mark.asyncio
async def test_pubsub_adapter_subscribe_passes_timeout() -> None:
    client = Mock(spec=RedisClient)
    codec = RedisPubSubCodec(model=_Payload)
    captured: dict[str, object] = {}

    async def _iter():
        yield ("orders", codec.encode(_Payload(value="hello")))

    def _subscribe(topics, timeout: Optional[timedelta] = None):
        captured["topics"] = topics
        captured["timeout"] = timeout
        return _iter()

    client.subscribe = Mock(side_effect=_subscribe)
    adapter = RedisPubSubAdapter(client=client, codec=codec)

    timeout = timedelta(seconds=5)
    stream = adapter.subscribe(["orders"], timeout=timeout)
    msg = await anext(stream)
    await stream.aclose()

    assert captured["topics"] == ["orders"]
    assert captured["timeout"] == timeout
    assert msg["payload"].value == "hello"
