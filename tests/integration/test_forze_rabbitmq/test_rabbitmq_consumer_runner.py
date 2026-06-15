"""End-to-end queue-consumer runner over a real RabbitMQ broker.

Composes the full outbox story: stage with bound correlation metadata ->
relay to RabbitMQ -> ``QueueConsumer.run`` (one-shot, finite idle timeout) ->
handler runs exactly once under the ORIGINAL correlation id. Plus the
transient-failure path: a failed-once handler is nacked back and succeeds
on the broker redelivery within the same run.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

pytest.importorskip("aio_pika")

from forze.application.contracts.crypto import (
    KeyRef,
    StaticKeyDirectory,
    is_encrypted_payload,
)
from forze.application.contracts.inbox import InboxDepKey, InboxSpec
from forze.application.contracts.outbox import (
    OutboxCommandDepKey,
    OutboxDestination,
    OutboxQueryDepKey,
    OutboxSpec,
)
from forze.application.execution import CryptoDepsModule
from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueMessage,
    QueueQueryDepKey,
    QueueSpec,
)
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import (
    Deps,
    DepsRegistry,
    ExecutionRuntime,
    InvocationMetadata,
)
from forze.base.primitives import uuid7
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.consumer import ConsumerRunResult, QueueConsumer
from forze_kits.integrations.outbox import OutboxRelay
from forze_mock import MockKeyManagement, MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.execution.module import (
    ConfigurableMockInbox,
    ConfigurableMockOutboxCommand,
    ConfigurableMockOutboxQuery,
    MockDepsModule,
    mock_strict_txmanager,
)
from forze_rabbitmq.adapters import RabbitMQQueueAdapter

# ----------------------- #

_INBOX_SPEC = InboxSpec(name="events")


def _consumer_deps(queue_adapter: RabbitMQQueueAdapter) -> Deps:
    """Mock outbox/inbox/strict-tx with the REAL queue adapter on route ``jobs``.

    Strict tx matters: a failing handler must roll its inbox mark back, or
    the redelivery test could not retry.
    """

    mock_module = MockDepsModule(state=MockState(), strict_tx=True)

    return Deps.plain(
        {
            MockStateDepKey: mock_module.state,
            OutboxCommandDepKey: ConfigurableMockOutboxCommand(module=mock_module),
            OutboxQueryDepKey: ConfigurableMockOutboxQuery(module=mock_module),
            InboxDepKey: ConfigurableMockInbox(module=mock_module),
            TransactionManagerDepKey: mock_strict_txmanager,
        }
    ).merge(
        Deps.routed(
            {
                QueueCommandDepKey: {"jobs": lambda _ctx, _spec: queue_adapter},
                QueueQueryDepKey: {"jobs": lambda _ctx, _spec: queue_adapter},
            }
        )
    )


# ----------------------- #


@pytest.mark.asyncio
async def test_runner_consumes_relayed_event_under_original_correlation(
    rabbitmq_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    codec = PydanticModelCodec(queue_payload_cls)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(_consumer_deps(rabbitmq_queue)).freeze()
    )

    original = InvocationMetadata(
        execution_id=uuid7(),
        correlation_id=uuid7(),
        causation_id=uuid7(),
    )

    async with runtime.scope():
        ctx = runtime.get_context()

        # 1. Stage inside an "operation" with bound correlation metadata.
        with ctx.inv_ctx.bind_metadata(metadata=original):
            await ctx.outbox.command(outbox_spec).stage(
                "job.requested", queue_payload_cls(value="consume-me")
            )
            await ctx.outbox.command(outbox_spec).flush()

        # 2. Relay to RabbitMQ.
        relayed = await OutboxRelay(outbox_spec=outbox_spec, reclaim_stale_after=None).to_queue(ctx, queue_spec)
        assert relayed.published == 1

        # 3. One-shot consume: the runner replaces the hand-rolled loop.
        observed: dict[str, Any] = {}

        async def handler(message: QueueMessage[Any]) -> None:
            observed["value"] = message.payload.value
            observed["metadata"] = ctx.inv_ctx.get_metadata()
            observed["event_id"] = message.key

        consumer_metadata = InvocationMetadata(
            execution_id=uuid7(),
            correlation_id=uuid7(),  # consumer's own id, must be replaced
        )

        with ctx.inv_ctx.bind_metadata(metadata=consumer_metadata):
            result = await QueueConsumer(
                queue="jobs",
                queue_spec=queue_spec,
                handler=handler,
                inbox_spec=_INBOX_SPEC,
                tx_route="mock",
            ).run(ctx, timeout=timedelta(seconds=2))

        assert result == ConsumerRunResult(processed=1)
        assert observed["value"] == "consume-me"

        # The handler ran under the ORIGINAL correlation id, caused by the
        # consumed event (A1's envelope rebinding, through the runner).
        metadata = observed["metadata"]
        assert metadata is not None
        assert metadata.correlation_id == original.correlation_id
        assert metadata.causation_id == UUID(observed["event_id"])

        # 4. Acked: the queue is drained — a fresh receive finds nothing.
        assert (
            await rabbitmq_queue.receive("jobs", limit=1, timeout=timedelta(seconds=1))
            == []
        )


# ....................... #


@pytest.mark.asyncio
async def test_failed_once_handler_succeeds_on_broker_redelivery(
    rabbitmq_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    codec = PydanticModelCodec(queue_payload_cls)
    queue_spec = QueueSpec(name="jobs", codec=codec)
    queue = f"retry-{uuid4().hex[:8]}"

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(_consumer_deps(rabbitmq_queue)).freeze()
    )

    async with runtime.scope():
        ctx = runtime.get_context()

        await rabbitmq_queue.enqueue(
            queue, queue_payload_cls(value="flaky"), key=str(uuid7())
        )

        deliveries: list[int | None] = []

        async def handler(message: QueueMessage[Any]) -> None:
            deliveries.append(message.delivery_count)

            if len(deliveries) == 1:
                raise RuntimeError("transient")

        result = await QueueConsumer(
            queue=queue,
            queue_spec=queue_spec,
            handler=handler,
            inbox_spec=_INBOX_SPEC,
            tx_route="mock",
        ).run(ctx, timeout=timedelta(seconds=3))

        # nack(requeue=True) -> broker redelivery (redelivered flag -> count 2)
        # -> the strict-tx inbox mark rolled back, so the retry processed.
        assert deliveries == [1, 2]
        assert result == ConsumerRunResult(processed=1, failed=1)

        assert (
            await rabbitmq_queue.receive(queue, limit=1, timeout=timedelta(seconds=1))
            == []
        )


# ----------------------- #


def _e2e_consumer_deps(queue_adapter: RabbitMQQueueAdapter) -> Deps:
    """Like :func:`_consumer_deps`, plus a keyring for end-to-end encryption."""

    mock_module = MockDepsModule(state=MockState(), strict_tx=True)

    return Deps.merge(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="events-cmk")),
        )(),
        Deps.plain(
            {
                MockStateDepKey: mock_module.state,
                OutboxCommandDepKey: ConfigurableMockOutboxCommand(module=mock_module),
                OutboxQueryDepKey: ConfigurableMockOutboxQuery(module=mock_module),
                InboxDepKey: ConfigurableMockInbox(module=mock_module),
                TransactionManagerDepKey: mock_strict_txmanager,
            }
        ),
        Deps.routed(
            {
                QueueCommandDepKey: {"jobs": lambda _ctx, _spec: queue_adapter},
                QueueQueryDepKey: {"jobs": lambda _ctx, _spec: queue_adapter},
            }
        ),
    )


@pytest.mark.asyncio
async def test_end_to_end_encrypted_event_relayed_through_rabbitmq_and_decrypted(
    rabbitmq_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    """end_to_end: ciphertext stored, relayed through real RabbitMQ, decrypted by the runner."""

    codec = PydanticModelCodec(queue_payload_cls)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
        encryption="end_to_end",
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(_e2e_consumer_deps(rabbitmq_queue)).freeze()
    )

    async with runtime.scope():
        ctx = runtime.get_context()

        await ctx.outbox.command(outbox_spec).stage(
            "job.requested", queue_payload_cls(value="secret-cargo")
        )
        await ctx.outbox.command(outbox_spec).flush()

        # Ciphertext at rest in the outbox store (staging encrypted it).
        state = ctx.deps.provide(MockStateDepKey)
        assert is_encrypted_payload(state.outbox_rows["events"][0].payload)

        relayed = await OutboxRelay(outbox_spec=outbox_spec, reclaim_stale_after=None).to_queue(ctx, queue_spec)
        assert relayed.published == 1

        observed: dict[str, Any] = {}

        async def handler(message: QueueMessage[Any]) -> None:
            observed["value"] = message.payload.value

        result = await QueueConsumer(
            queue="jobs",
            queue_spec=queue_spec,
            handler=handler,
            inbox_spec=_INBOX_SPEC,
            tx_route="mock",
        ).run(ctx, timeout=timedelta(seconds=4))

        assert result == ConsumerRunResult(processed=1)
        assert observed["value"] == "secret-cargo"
