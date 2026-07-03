"""Kafka producer adapter: position id, native key/headers/timestamp mapping."""

from datetime import datetime, timezone

from forze_kafka.adapters import KafkaStreamCommandAdapter

from _kafka_fakes import FakeKafkaClient, Msg, make_codec

# ----------------------- #


def _adapter(client: FakeKafkaClient) -> KafkaStreamCommandAdapter[Msg]:
    return KafkaStreamCommandAdapter(
        client=client,  # type: ignore[arg-type]
        codec=make_codec(),
        namespace="",
        tenant_aware=False,
        tenant_provider=lambda: None,
    )


async def test_append_returns_canonical_position_id() -> None:
    client = FakeKafkaClient(send_partition=2, send_offset=5)
    adapter = _adapter(client)

    message_id = await adapter.append(
        "events",
        Msg(body="x"),
        type="evt",
        key="k1",
        headers={"forze_event_id": "e1"},
    )

    assert message_id == "events:2:5"
    sent = client.sends[0]
    assert sent["topic"] == "events"
    assert sent["key"] == b"k1"
    assert ("forze_type", b"evt") in sent["headers"]
    assert ("forze_event_id", b"e1") in sent["headers"]


async def test_append_maps_timestamp_to_millis() -> None:
    client = FakeKafkaClient()
    adapter = _adapter(client)
    when = datetime(2021, 6, 1, tzinfo=timezone.utc)

    await adapter.append("events", Msg(body="x"), timestamp=when)

    assert client.sends[0]["timestamp_ms"] == int(when.timestamp() * 1000)


async def test_append_without_key_or_timestamp() -> None:
    client = FakeKafkaClient()
    adapter = _adapter(client)

    await adapter.append("events", Msg(body="x"))

    assert client.sends[0]["key"] is None
    assert client.sends[0]["timestamp_ms"] is None


async def test_append_namespaced_topic() -> None:
    client = FakeKafkaClient()
    adapter = KafkaStreamCommandAdapter(
        client=client,  # type: ignore[arg-type]
        codec=make_codec(),
        namespace="tenantA",
        tenant_aware=False,
        tenant_provider=lambda: None,
    )

    await adapter.append("events", Msg(body="x"))

    assert client.sends[0]["topic"] == "tenantA.events"
