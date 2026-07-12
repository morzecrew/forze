"""End-to-end envelope tracing across the SQS (floci emulator) broker boundary.

Mirrors the RabbitMQ showcase: stage with bound correlation metadata ->
relay -> consume -> ``process_with_inbox`` rebinds the original correlation
chain. Also covers the SQS specifics: message-attribute round-trip and
``ApproximateReceiveCount`` -> ``delivery_count`` on redelivery.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

pytest.importorskip("aioboto3")

from forze.application.contracts.envelope import (
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_TENANT_ID,
)
from forze.application.contracts.inbox import InboxDepKey, InboxSpec
from forze.application.contracts.outbox import (
    OutboxCommandDepKey,
    OutboxDestination,
    OutboxQueryDepKey,
    OutboxSpec,
)
from forze.application.contracts.queue import (
    QueueCommandDepKey,
    QueueMessage,
    QueueSpec,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import (
    Deps,
    DepsRegistry,
    ExecutionRuntime,
    InvocationMetadata,
)
from forze.base.primitives import uuid7
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.inbox import process_with_inbox
from forze_kits.integrations.outbox import OutboxRelay
from forze_mock import MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.execution.module import (
    ConfigurableMockInbox,
    ConfigurableMockOutboxCommand,
    ConfigurableMockOutboxQuery,
    MockDepsModule,
    mock_txmanager,
)
from forze_sqs.adapters import SQSQueueAdapter
from forze_sqs.kernel.client import SQSClient

# ----------------------- #


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


def _tracing_deps(queue_adapter: SQSQueueAdapter) -> Deps:
    """Mock outbox/inbox/tx with the REAL queue adapter as relay command."""

    mock_module = MockDepsModule(state=MockState())

    return Deps.plain(
        {
            MockStateDepKey: mock_module.state,
            OutboxCommandDepKey: ConfigurableMockOutboxCommand(module=mock_module),
            OutboxQueryDepKey: ConfigurableMockOutboxQuery(module=mock_module),
            InboxDepKey: ConfigurableMockInbox(module=mock_module),
            TransactionManagerDepKey: mock_txmanager,
        }
    ).merge(
        Deps.routed(
            {QueueCommandDepKey: {"jobs": lambda _ctx, _spec: queue_adapter}}
        )
    )


# ----------------------- #


async def _receive_until(
    adapter: SQSQueueAdapter,
    queue: str,
    *,
    attempts: int = 10,
):
    for _ in range(attempts):
        messages = await adapter.receive(queue, limit=1, timeout=timedelta(seconds=2))

        if messages:
            return messages[0]

    raise AssertionError("Queue message was not received in time")


# ----------------------- #


@pytest.mark.asyncio
async def test_end_to_end_correlation_survives_the_sqs_hop(
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

    # The relay publishes through the REAL SQS adapter; outbox, inbox, and tx
    # stay in-memory.
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(_tracing_deps(sqs_queue)).freeze()
    )

    original = InvocationMetadata(
        execution_id=uuid7(),
        correlation_id=uuid7(),
        causation_id=uuid7(),
    )
    tenant = TenantIdentity(tenant_id=uuid4())

    async with runtime.scope():
        ctx = runtime.get_context()

        with ctx.inv_ctx.bind(metadata=original, tenant=tenant):
            await ctx.outbox.command(outbox_spec).stage(
                "job.requested", queue_payload_cls(value="trace-me")
            )
            await ctx.outbox.command(outbox_spec).flush()

        result = await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=None
        ).to_queue(ctx, queue_spec)
        assert result.published == 1

        message: QueueMessage[Any] = await _receive_until(sqs_queue, "jobs")

        assert message.payload.value == "trace-me"
        assert message.headers[HEADER_CORRELATION_ID] == str(original.correlation_id)
        assert message.headers[HEADER_TENANT_ID] == str(tenant.tenant_id)
        assert message.headers[HEADER_EVENT_ID] == message.key
        assert message.delivery_count == 1

        observed: dict[str, Any] = {}

        async def handler(msg: QueueMessage[Any]) -> None:
            observed["metadata"] = ctx.inv_ctx.get_metadata()

        processed = await process_with_inbox(
            ctx,
            message,
            inbox_spec=InboxSpec(name="events"),
            handler=handler,
            tx_route="mock",
        )

        assert processed is True
        metadata = observed["metadata"]
        assert metadata is not None
        assert metadata.correlation_id == original.correlation_id
        assert metadata.causation_id == UUID(message.headers[HEADER_EVENT_ID])

        await sqs_queue.ack("jobs", [message.id])


# ....................... #


@pytest.mark.asyncio
async def test_attributes_round_trip_and_receive_count_on_redelivery(
    sqs_client: SQSClient,
    sqs_queue: SQSQueueAdapter,
    queue_payload_cls,
) -> None:
    queue = f"hdrs-{uuid4().hex[:8]}"
    await _ensure_queue(sqs_client, sqs_queue, queue)

    await sqs_queue.enqueue(
        queue,
        queue_payload_cls(value="x"),
        type="created",
        key="real-key",
        headers={
            "trace": "t-1",
            "forze_correlation_id": "corr-1",
            "forze_key": "forged-key",  # reserved: transport value must win
        },
    )

    first = await _receive_until(sqs_queue, queue)

    assert first.headers["trace"] == "t-1"
    assert first.headers["forze_correlation_id"] == "corr-1"
    assert first.key == "real-key"
    assert "forze_key" not in first.headers
    # ApproximateReceiveCount of the first delivery.
    assert first.delivery_count == 1

    # Visibility reset -> immediate redelivery -> count increments.
    await sqs_queue.nack(queue, [first.id], requeue=True)

    second = await _receive_until(sqs_queue, queue)
    assert second.id == first.id
    assert second.delivery_count == 2
    # Headers survive redelivery untouched.
    assert second.headers["trace"] == "t-1"

    await sqs_queue.ack(queue, [second.id])
