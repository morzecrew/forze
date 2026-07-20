"""Real-broker offset-log differential: produce/consume/commit, ordering, replay, lag.

The Kafka-side of RFC 0007's conformance battery — the mock proves the semantics
against itself; this proves the aiokafka adapter agrees with a live broker.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import uuid4

from _kafka_models import Payload

from forze.application.contracts.stream import OffsetReset, StreamPosition
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.serialization import PydanticModelCodec
from forze_kafka.adapters import (
    KafkaCommitStreamGroupAdapter,
    KafkaCommitStreamGroupAdminAdapter,
    KafkaStreamCodec,
    KafkaStreamCommandAdapter,
)
from forze_kafka.kernel.client import KafkaClient, KafkaConfig

# ----------------------- #


async def _read_until(
    consumer: KafkaCommitStreamGroupAdapter[Payload],
    group: str,
    member: str,
    topics: list[str],
    *,
    minimum: int,
    attempts: int = 20,
) -> list[StreamPosition]:
    collected: list[Payload] = []
    positions: list[StreamPosition] = []

    for _ in range(attempts):
        batch = await consumer.read(group, member, topics, timeout=timedelta(seconds=1))

        for message in batch:
            collected.append(message.payload)
            positions.append(StreamPosition.from_message(message))

        if len(collected) >= minimum:
            break

    assert len(collected) >= minimum, f"expected >= {minimum}, got {len(collected)}"
    return positions


async def _bodies(
    consumer: KafkaCommitStreamGroupAdapter[Payload],
    group: str,
    member: str,
    topic: str,
    *,
    minimum: int,
) -> list[str]:
    out: list[str] = []

    for _ in range(20):
        batch = await consumer.read(
            group, member, [topic], timeout=timedelta(seconds=1)
        )
        out.extend(m.payload.value for m in batch)

        if len(out) >= minimum:
            break

    return out


@asynccontextmanager
async def _dedicated_consumer(
    bootstrap: str,
) -> AsyncIterator[KafkaCommitStreamGroupAdapter[Payload]]:
    """A consumer on its own client, closed on exit — so it leaves the group cleanly.

    Replay and crash-recovery must not have a stale live member holding the
    partition when the next consumer joins (that triggers a rebalance and the
    stale member, positioned at the log end, starves the newcomer).
    """

    client = KafkaClient()
    await client.initialize(bootstrap, config=KafkaConfig(auto_offset_reset="earliest"))
    try:
        yield KafkaCommitStreamGroupAdapter(
            client=client,
            codec=KafkaStreamCodec(
                payload_codec=PydanticModelCodec(model_type=Payload)
            ),
            namespace="",
            tenant_aware=False,
            tenant_provider=lambda: None,
            auto_offset_reset="earliest",
        )
    finally:
        await client.close()


# ....................... #


async def test_produce_consume_commit_then_lag_zero(
    producer: KafkaStreamCommandAdapter[Payload],
    consumer: KafkaCommitStreamGroupAdapter[Payload],
    admin: KafkaCommitStreamGroupAdminAdapter,
) -> None:
    topic = f"it-events-{uuid4().hex[:8]}"
    group = f"g-{uuid4().hex[:8]}"
    await admin.ensure_topic(topic, partitions=1)

    for i in range(3):
        message_id = await producer.append(topic, Payload(value=str(i)), key="k")
        assert message_id.startswith(f"{topic}:")

    positions = await _read_until(consumer, group, "m1", [topic], minimum=3)
    await consumer.commit(group, positions)

    lags = await admin.lag(group, topic)
    assert lags, "expected per-partition lag rows"
    assert sum(lag.lag for lag in lags) == 0


async def test_per_partition_ordering(
    producer: KafkaStreamCommandAdapter[Payload],
    consumer: KafkaCommitStreamGroupAdapter[Payload],
    admin: KafkaCommitStreamGroupAdminAdapter,
) -> None:
    topic = f"it-order-{uuid4().hex[:8]}"
    group = f"g-{uuid4().hex[:8]}"
    await admin.ensure_topic(topic, partitions=3)

    # Same key → same partition → produce order preserved on that partition.
    for i in range(6):
        await producer.append(topic, Payload(value=str(i)), key="same-key")

    bodies = await _bodies(consumer, group, "m1", topic, minimum=6)

    assert bodies[:6] == [str(i) for i in range(6)]


async def test_replay_reset_to_earliest(
    kafka_container,
    producer: KafkaStreamCommandAdapter[Payload],
    admin: KafkaCommitStreamGroupAdminAdapter,
) -> None:
    topic = f"it-replay-{uuid4().hex[:8]}"
    group = f"g-{uuid4().hex[:8]}"
    bootstrap = kafka_container.get_bootstrap_server()
    await admin.ensure_topic(topic, partitions=1)

    for i in range(3):
        await producer.append(topic, Payload(value=str(i)), key="k")

    # Consume + commit, then leave the group (close the client).
    async with _dedicated_consumer(bootstrap) as first:
        positions = await _read_until(first, group, "m1", [topic], minimum=3)
        await first.commit(group, positions)

    # Replay: rewind the committed cursor while consumers are down.
    await admin.reset_offsets(group, topic, to=OffsetReset.EARLIEST)

    # A fresh consumer resumes from the reset cursor → the same records replay.
    async with _dedicated_consumer(bootstrap) as second:
        bodies = await _bodies(second, group, "m2", topic, minimum=3)

    assert sorted(bodies[:3]) == ["0", "1", "2"]


async def test_at_least_once_redelivery_without_commit(
    kafka_container,
    producer: KafkaStreamCommandAdapter[Payload],
    admin: KafkaCommitStreamGroupAdminAdapter,
) -> None:
    topic = f"it-alo-{uuid4().hex[:8]}"
    group = f"g-{uuid4().hex[:8]}"
    bootstrap = kafka_container.get_bootstrap_server()
    await admin.ensure_topic(topic, partitions=1)

    for i in range(3):
        await producer.append(topic, Payload(value=str(i)), key="k")

    # Consume but never commit, then crash (close the client mid-flight).
    async with _dedicated_consumer(bootstrap) as crashing:
        await _read_until(crashing, group, "m1", [topic], minimum=3)

    # A fresh consumer in the same group has no committed offset → redelivery.
    async with _dedicated_consumer(bootstrap) as recovered:
        bodies = await _bodies(recovered, group, "m-recover", topic, minimum=3)

    assert sorted(bodies[:3]) == ["0", "1", "2"]


async def test_namespaced_tenant_round_trip(
    kafka_client,
) -> None:
    # tenant_aware=True + namespace → the physical topic is prefixed per tenant.
    tenant = uuid4()
    codec = KafkaStreamCodec(payload_codec=PydanticModelCodec(model_type=Payload))
    provider = lambda: TenantIdentity(tenant_id=tenant)  # noqa: E731
    namespace = "tns"

    producer = KafkaStreamCommandAdapter(
        client=kafka_client,
        codec=codec,
        namespace=namespace,
        tenant_aware=True,
        tenant_provider=provider,
    )
    consumer = KafkaCommitStreamGroupAdapter(
        client=kafka_client,
        codec=codec,
        namespace=namespace,
        tenant_aware=True,
        tenant_provider=provider,
        auto_offset_reset="earliest",
    )
    admin = KafkaCommitStreamGroupAdminAdapter(
        client=kafka_client,
        namespace=namespace,
        tenant_aware=True,
        tenant_provider=provider,
    )

    topic = f"ev-{uuid4().hex[:8]}"
    group = f"g-{uuid4().hex[:8]}"
    await admin.ensure_topic(topic, partitions=1)

    message_id = await producer.append(topic, Payload(value="x"), key="k")
    assert message_id.startswith(f"{namespace}.{topic}:")  # tenant-namespaced topic

    positions: list[StreamPosition] = []
    for _ in range(20):
        batch = await consumer.read(group, "m1", [topic], timeout=timedelta(seconds=1))
        positions.extend(StreamPosition.from_message(m) for m in batch)
        if positions:
            break
    assert positions
    await consumer.commit(group, positions)

    lags = await admin.lag(group, topic)
    assert lags and sum(lag.lag for lag in lags) == 0


async def test_admin_on_missing_topic(
    admin: KafkaCommitStreamGroupAdminAdapter,
) -> None:
    topic = f"nope-{uuid4().hex[:8]}"
    group = f"g-{uuid4().hex[:8]}"

    # No partitions to discover → lag is empty and replay is a safe no-op.
    assert await admin.lag(group, topic) == []
    await admin.reset_offsets(group, topic, to=OffsetReset.EARLIEST)


async def test_capabilities_and_ensure_topic_idempotent(
    admin: KafkaCommitStreamGroupAdminAdapter,
    consumer: KafkaCommitStreamGroupAdapter[Payload],
) -> None:
    topic = f"it-caps-{uuid4().hex[:8]}"
    await admin.ensure_topic(topic, partitions=2)
    await admin.ensure_topic(topic, partitions=2)  # idempotent, no raise

    assert admin.capabilities().supports_replay is True
    assert consumer.capabilities().supports_transactions is False
