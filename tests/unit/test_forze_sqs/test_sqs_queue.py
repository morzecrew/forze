from datetime import datetime, timedelta
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
from pydantic import BaseModel

from forze_sqs.adapters import SQSQueueAdapter, SQSQueueCodec
from forze_sqs.kernel.platform import SQSClient


class _Payload(BaseModel):
    value: str


def test_queue_codec_encode_decode_roundtrip() -> None:
    codec = SQSQueueCodec(model=_Payload)
    ts = datetime(2025, 1, 1, 12, 0, 0)

    encoded = codec.encode(_Payload(value="hello"))
    decoded = codec.decode(
        "jobs",
        {
            "queue": "jobs",
            "id": "receipt-1",
            "body": encoded,
            "type": "created",
            "enqueued_at": ts,
            "key": "partition-a",
        },
    )

    assert decoded["queue"] == "jobs"
    assert decoded["id"] == "receipt-1"
    assert decoded["payload"].value == "hello"
    assert decoded["type"] == "created"
    assert decoded["enqueued_at"] == ts
    assert decoded["key"] == "partition-a"


@pytest.mark.asyncio
async def test_queue_adapter_enqueue_uses_namespaced_queue() -> None:
    client = Mock(spec=SQSClient)
    client.client = MagicMock(return_value=AsyncMock())
    client.enqueue = AsyncMock(return_value="msg-1")
    adapter = SQSQueueAdapter(
        client=client,
        codec=SQSQueueCodec(model=_Payload),
        namespace="ns:primary",
    )

    message_id = await adapter.enqueue("jobs", _Payload(value="hello"))

    assert message_id == "msg-1"
    client.enqueue.assert_awaited_once()
    assert client.enqueue.await_args.args[0] == "ns:primary-jobs"


@pytest.mark.asyncio
async def test_queue_adapter_enqueue_many_uses_namespaced_queue() -> None:
    client = Mock(spec=SQSClient)
    client.client = MagicMock(return_value=AsyncMock())
    client.enqueue_many = AsyncMock(return_value=["msg-1", "msg-2"])
    codec = SQSQueueCodec(model=_Payload)
    adapter = SQSQueueAdapter(client=client, codec=codec, namespace="ns:primary")

    message_ids = await adapter.enqueue_many(
        "jobs",
        [_Payload(value="hello"), _Payload(value="world")],
        type="created",
        key="partition-a",
    )

    assert message_ids == ["msg-1", "msg-2"]
    client.enqueue_many.assert_awaited_once()
    assert client.enqueue_many.await_args.args[0] == "ns:primary-jobs"
    bodies = client.enqueue_many.await_args.args[1]
    assert len(bodies) == 2
    assert codec.decode(
        "jobs",
        {
            "queue": "ns:primary-jobs",
            "id": "msg-1",
            "body": bodies[0],
            "type": None,
            "enqueued_at": None,
            "key": None,
        },
    )["payload"].value == "hello"
    assert codec.decode(
        "jobs",
        {
            "queue": "ns:primary-jobs",
            "id": "msg-2",
            "body": bodies[1],
            "type": None,
            "enqueued_at": None,
            "key": None,
        },
    )["payload"].value == "world"
    assert client.enqueue_many.await_args.kwargs["type"] == "created"
    assert client.enqueue_many.await_args.kwargs["key"] == "partition-a"


@pytest.mark.asyncio
async def test_queue_adapter_enqueue_many_with_empty_payloads() -> None:
    client = Mock(spec=SQSClient)
    client.enqueue_many = AsyncMock()
    adapter = SQSQueueAdapter(
        client=client,
        codec=SQSQueueCodec(model=_Payload),
        namespace="ns",
    )

    message_ids = await adapter.enqueue_many("jobs", [])

    assert message_ids == []
    client.enqueue_many.assert_not_awaited()


@pytest.mark.asyncio
async def test_queue_adapter_receive_decodes_messages() -> None:
    client = Mock(spec=SQSClient)
    client.client = MagicMock(return_value=AsyncMock())
    codec = SQSQueueCodec(model=_Payload)
    ts = datetime(2025, 1, 1, 12, 0, 0)
    client.receive = AsyncMock(
        return_value=[
            {
                "queue": "ns:jobs",
                "id": "receipt-1",
                "body": codec.encode(_Payload(value="hello")),
                "type": "created",
                "enqueued_at": ts,
                "key": "partition-a",
            }
        ]
    )
    adapter = SQSQueueAdapter(client=client, codec=codec, namespace="ns")

    timeout = timedelta(seconds=2)
    messages = await adapter.receive("jobs", limit=2, timeout=timeout)

    assert len(messages) == 1
    assert messages[0]["queue"] == "jobs"
    assert messages[0]["payload"].value == "hello"
    assert messages[0]["type"] == "created"
    assert messages[0]["enqueued_at"] == ts
    assert messages[0]["key"] == "partition-a"
    client.receive.assert_awaited_once_with("ns-jobs", limit=2, timeout=timeout)


@pytest.mark.asyncio
async def test_queue_adapter_consume_decodes_messages() -> None:
    client = Mock(spec=SQSClient)
    client.client = MagicMock(return_value=AsyncMock())
    codec = SQSQueueCodec(model=_Payload)
    captured: dict[str, object] = {}

    async def _iter():
        yield {
            "queue": "ns:jobs",
            "id": "receipt-1",
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
    adapter = SQSQueueAdapter(client=client, codec=codec, namespace="ns")

    timeout = timedelta(seconds=1)
    stream = adapter.consume("jobs", timeout=timeout)
    message = await anext(stream)
    await stream.aclose()

    assert captured["queue"] == "ns-jobs"
    assert captured["timeout"] == timeout
    assert message["queue"] == "jobs"
    assert message["payload"].value == "hello"


@pytest.mark.asyncio
async def test_queue_adapter_ack_and_nack_use_namespaced_queue() -> None:
    client = Mock(spec=SQSClient)
    client.client = MagicMock(return_value=AsyncMock())
    client.ack = AsyncMock(return_value=1)
    client.nack = AsyncMock(return_value=1)
    adapter = SQSQueueAdapter(
        client=client,
        codec=SQSQueueCodec(model=_Payload),
        namespace="ns",
    )

    acked = await adapter.ack("jobs", ["receipt-1"])
    nacked = await adapter.nack("jobs", ["receipt-1"], requeue=False)

    assert acked == 1
    assert nacked == 1
    client.ack.assert_awaited_once_with("ns-jobs", ["receipt-1"])
    client.nack.assert_awaited_once_with("ns-jobs", ["receipt-1"], requeue=False)
