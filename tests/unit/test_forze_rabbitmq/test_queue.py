from datetime import datetime, timedelta
from typing import Optional
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import BaseModel

from forze_rabbitmq.adapters import RabbitMQQueueAdapter, RabbitMQQueueCodec
from forze_rabbitmq.kernel.platform import RabbitMQClient


class _Payload(BaseModel):
    value: str


def test_queue_codec_encode_decode_roundtrip() -> None:
    codec = RabbitMQQueueCodec(model=_Payload)
    ts = datetime(2025, 1, 1, 12, 0, 0)

    encoded = codec.encode(_Payload(value="hello"))
    decoded = codec.decode(
        "jobs",
        {
            "queue": "jobs",
            "id": "msg-1",
            "body": encoded,
            "type": "created",
            "enqueued_at": ts,
            "key": "partition-a",
        },
    )

    assert decoded["queue"] == "jobs"
    assert decoded["id"] == "msg-1"
    assert decoded["payload"].value == "hello"
    assert decoded["type"] == "created"
    assert decoded["enqueued_at"] == ts
    assert decoded["key"] == "partition-a"


@pytest.mark.asyncio
async def test_queue_adapter_enqueue_uses_namespaced_queue() -> None:
    client = Mock(spec=RabbitMQClient)
    client.enqueue = AsyncMock(return_value="msg-1")
    adapter = RabbitMQQueueAdapter(
        client=client,
        codec=RabbitMQQueueCodec(model=_Payload),
        namespace="ns",
    )

    message_id = await adapter.enqueue("jobs", _Payload(value="hello"))

    assert message_id == "msg-1"
    client.enqueue.assert_awaited_once()
    assert client.enqueue.await_args.args[0] == "ns:jobs"


@pytest.mark.asyncio
async def test_queue_adapter_receive_decodes_messages() -> None:
    client = Mock(spec=RabbitMQClient)
    codec = RabbitMQQueueCodec(model=_Payload)
    ts = datetime(2025, 1, 1, 12, 0, 0)
    client.receive = AsyncMock(
        return_value=[
            {
                "queue": "ns:jobs",
                "id": "msg-1",
                "body": codec.encode(_Payload(value="hello")),
                "type": "created",
                "enqueued_at": ts,
                "key": "partition-a",
            }
        ]
    )
    adapter = RabbitMQQueueAdapter(client=client, codec=codec, namespace="ns")

    timeout = timedelta(seconds=2)
    messages = await adapter.receive("jobs", limit=2, timeout=timeout)

    assert len(messages) == 1
    assert messages[0]["queue"] == "jobs"
    assert messages[0]["payload"].value == "hello"
    assert messages[0]["type"] == "created"
    assert messages[0]["enqueued_at"] == ts
    assert messages[0]["key"] == "partition-a"
    client.receive.assert_awaited_once_with("ns:jobs", limit=2, timeout=timeout)


@pytest.mark.asyncio
async def test_queue_adapter_consume_decodes_messages() -> None:
    client = Mock(spec=RabbitMQClient)
    codec = RabbitMQQueueCodec(model=_Payload)
    captured: dict[str, object] = {}

    async def _iter():
        yield {
            "queue": "ns:jobs",
            "id": "msg-1",
            "body": codec.encode(_Payload(value="hello")),
            "type": None,
            "enqueued_at": None,
            "key": None,
        }

    def _consume(queue: str, timeout: Optional[timedelta] = None):
        captured["queue"] = queue
        captured["timeout"] = timeout
        return _iter()

    client.consume = Mock(side_effect=_consume)
    adapter = RabbitMQQueueAdapter(client=client, codec=codec, namespace="ns")

    timeout = timedelta(seconds=1)
    stream = adapter.consume("jobs", timeout=timeout)
    message = await anext(stream)
    await stream.aclose()

    assert captured["queue"] == "ns:jobs"
    assert captured["timeout"] == timeout
    assert message["queue"] == "jobs"
    assert message["payload"].value == "hello"


@pytest.mark.asyncio
async def test_queue_adapter_ack_and_nack_use_namespaced_queue() -> None:
    client = Mock(spec=RabbitMQClient)
    client.ack = AsyncMock(return_value=1)
    client.nack = AsyncMock(return_value=1)
    adapter = RabbitMQQueueAdapter(
        client=client,
        codec=RabbitMQQueueCodec(model=_Payload),
        namespace="ns",
    )

    acked = await adapter.ack("jobs", ["msg-1"])
    nacked = await adapter.nack("jobs", ["msg-1"], requeue=False)

    assert acked == 1
    assert nacked == 1
    client.ack.assert_awaited_once_with("ns:jobs", ["msg-1"])
    client.nack.assert_awaited_once_with("ns:jobs", ["msg-1"], requeue=False)
