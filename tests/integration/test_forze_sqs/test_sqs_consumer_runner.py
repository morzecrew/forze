"""End-to-end queue-consumer runner over SQS (LocalStack).

Mirrors the RabbitMQ runner showcase: stage with bound correlation
metadata -> relay to SQS -> ``QueueConsumer.run`` (one-shot, finite idle
timeout) -> handler runs exactly once under the ORIGINAL correlation id,
and the queue is drained (acked).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("aioboto3")

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
from forze_sqs.adapters import SQSQueueAdapter
from forze_sqs.kernel.client import SQSClient

# ----------------------- #

_INBOX_SPEC = InboxSpec(name="events")


async def _ensure_queue(
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue: str,
) -> None:
    async with sqs_client.client():
        physical_queue = (
            f"{sqs_queue.namespace}-{queue}" if sqs_queue.namespace else queue
        )
        await sqs_client.create_queue(physical_queue)


def _consumer_deps(queue_adapter: SQSQueueAdapter) -> Deps:
    """Mock outbox/inbox/strict-tx with the REAL queue adapter on route ``jobs``."""

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
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue_payload_cls,
) -> None:
    await _ensure_queue(sqs_client, sqs_queue, "jobs")

    codec = PydanticModelCodec(queue_payload_cls)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(_consumer_deps(sqs_queue)).freeze()
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

        # 2. Relay to SQS.
        relayed = await OutboxRelay(outbox_spec=outbox_spec, reclaim_stale_after=None).to_queue(ctx, queue_spec)
        assert relayed.published == 1

        # 3. One-shot consume: the runner replaces the hand-rolled loop.
        observed: dict[str, Any] = {}

        async def handler(message: QueueMessage[Any]) -> None:
            observed["value"] = message.payload.value
            observed["metadata"] = ctx.inv_ctx.get_metadata()
            observed["event_id"] = message.key
            observed["delivery_count"] = message.delivery_count

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
            ).run(ctx, timeout=timedelta(seconds=4))

        assert result == ConsumerRunResult(processed=1)
        assert observed["value"] == "consume-me"
        assert observed["delivery_count"] == 1  # ApproximateReceiveCount

        # The handler ran under the ORIGINAL correlation id, caused by the
        # consumed event (A1's envelope rebinding, through the runner).
        metadata = observed["metadata"]
        assert metadata is not None
        assert metadata.correlation_id == original.correlation_id
        assert metadata.causation_id == UUID(observed["event_id"])

        # 4. Acked: the queue is drained — a fresh receive finds nothing.
        assert (
            await sqs_queue.receive("jobs", limit=1, timeout=timedelta(seconds=2)) == []
        )


# ----------------------- #


def _e2e_consumer_deps(queue_adapter: SQSQueueAdapter) -> Deps:
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
async def test_end_to_end_encrypted_event_relayed_through_sqs_and_decrypted(
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue_payload_cls,
) -> None:
    """end_to_end: ciphertext is stored, relayed through real SQS, decrypted by the runner."""

    await _ensure_queue(sqs_client, sqs_queue, "jobs")

    codec = PydanticModelCodec(queue_payload_cls)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
        encryption="end_to_end",
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(_e2e_consumer_deps(sqs_queue)).freeze()
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

        # Relay forwards the ciphertext through real SQS (no relay decrypt for e2e).
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

        # The consumer decrypted the SQS-delivered ciphertext to the plaintext model.
        assert result == ConsumerRunResult(processed=1)
        assert observed["value"] == "secret-cargo"
