"""KafkaClient guards on the real (uninitialized) client + the tail poll loop.

These exercise real objects, not mocks: an uninitialized ``KafkaClient`` and the
consumer ``tail`` loop over a thin recording fake.
"""

import pytest
from _kafka_fakes import FakeConsumer, FakeKafkaClient, Msg, make_codec, record
from aiokafka.structs import TopicPartition

from forze.base.exceptions import CoreException
from forze_kafka.adapters import KafkaCommitStreamGroupAdapter
from forze_kafka.kernel.client import KafkaClient

# ----------------------- #


async def test_send_before_initialize_raises() -> None:
    with pytest.raises(CoreException):
        await KafkaClient().send("t", b"v")


async def test_admin_before_initialize_raises() -> None:
    with pytest.raises(CoreException):
        await KafkaClient().admin()


async def test_close_before_initialize_is_noop() -> None:
    client = KafkaClient()
    await client.close()  # no producer/admin/consumers → safe no-op
    await client.close()  # idempotent


async def test_group_config_defaults() -> None:
    config = KafkaClient().group_config()

    assert config["auto_offset_reset"] == "latest"


async def test_tail_polls_past_empty_reads() -> None:
    codec = make_codec()
    tp = TopicPartition("events", 0)
    # First poll empty (triggers the timeout=None backoff), then a message.
    consumer = FakeConsumer(
        batch_sequence=[
            {},
            {tp: [record("events", 0, 0, codec.encode_value(Msg(body="late")))]},
        ]
    )
    adapter: KafkaCommitStreamGroupAdapter[Msg] = KafkaCommitStreamGroupAdapter(
        client=FakeKafkaClient(consumer=consumer),  # type: ignore[arg-type]
        codec=codec,
        namespace="",
        tenant_aware=False,
        tenant_provider=lambda: None,
    )

    stream = adapter.tail("g", "m", ["events"], timeout=None)
    try:
        first = await anext(stream)
        second = await anext(stream)  # loops back after a message-bearing poll
    finally:
        await stream.aclose()

    assert first.payload.body == "late"
    assert second.payload.body == "late"
