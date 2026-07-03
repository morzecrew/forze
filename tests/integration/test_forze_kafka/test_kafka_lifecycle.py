"""Integration tests for the Kafka lifecycle step (startup init + shutdown close)."""

from __future__ import annotations

from forze.application.execution import Deps
from forze_kafka.execution.deps import KafkaClientDepKey
from forze_kafka.execution.lifecycle import kafka_lifecycle_step
from forze_kafka.kernel.client import KafkaClient

from tests.support.execution_context import context_from_deps

# ----------------------- #


async def test_lifecycle_startup_initializes_and_shutdown_closes(
    kafka_container,  # noqa: ANN001 - session container fixture
) -> None:
    client = KafkaClient()
    ctx = context_from_deps(Deps.plain({KafkaClientDepKey: client}))
    step = kafka_lifecycle_step(
        bootstrap_servers=kafka_container.get_bootstrap_server()
    )

    await step.startup(ctx)  # KafkaStartupHook → client.initialize
    try:
        name, healthy = await client.health()
        assert name == "Kafka"
        assert healthy is True

        # A second get_consumer for the same (group, member, topics) is pooled.
        first = await client.get_consumer(group="g", member="m", topics=["lc-topic"])
        second = await client.get_consumer(group="g", member="m", topics=["lc-topic"])
        assert first is second
    finally:
        await step.shutdown(ctx)  # KafkaShutdownHook → client.close

    # After shutdown the producer is torn down.
    assert (await client.health())[1] is False
