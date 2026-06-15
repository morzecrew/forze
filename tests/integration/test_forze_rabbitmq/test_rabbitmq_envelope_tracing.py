"""End-to-end envelope tracing across the RabbitMQ broker boundary.

The showcase flow for header propagation: an event staged inside an
operation with bound correlation metadata is relayed to RabbitMQ, consumed,
and processed via ``process_with_inbox`` — the handler must run under the
ORIGINAL correlation id with ``causation_id == event_id``.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

pytest.importorskip("aio_pika")

from forze.application.contracts.envelope import (
    HEADER_CORRELATION_ID,
    HEADER_EVENT_ID,
    HEADER_EXECUTION_ID,
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
from forze.base.serialization import PydanticModelCodec
from forze.base.primitives import uuid7
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
from forze_rabbitmq.adapters import RabbitMQQueueAdapter

# ----------------------- #


def _tracing_deps(queue_adapter: RabbitMQQueueAdapter) -> Deps:
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
    adapter: RabbitMQQueueAdapter,
    queue: str,
    *,
    attempts: int = 8,
):
    for _ in range(attempts):
        messages = await adapter.receive(queue, limit=1, timeout=timedelta(seconds=1))

        if messages:
            return messages[0]

    raise AssertionError("Queue message was not received in time")


# ----------------------- #


@pytest.mark.asyncio
async def test_end_to_end_correlation_survives_the_rabbitmq_hop(
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

    # The relay publishes through the REAL RabbitMQ adapter; outbox, inbox,
    # and tx stay in-memory.
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_deps(_tracing_deps(rabbitmq_queue)).freeze()
    )

    original = InvocationMetadata(
        execution_id=uuid7(),
        correlation_id=uuid7(),
        causation_id=uuid7(),
    )
    tenant = TenantIdentity(tenant_id=uuid4())

    async with runtime.scope():
        ctx = runtime.get_context()

        # 1. Stage inside an "operation" with bound correlation metadata.
        with ctx.inv_ctx.bind(metadata=original, tenant=tenant):
            await ctx.outbox.command(outbox_spec).stage(
                "job.requested", queue_payload_cls(value="trace-me")
            )
            await ctx.outbox.command(outbox_spec).flush()

        # 2. Relay to RabbitMQ.
        result = await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=None
        ).to_queue(ctx, queue_spec)
        assert result.published == 1

        # 3. Consume: envelope headers rode the AMQP message headers.
        message: QueueMessage[Any] = await _receive_until(rabbitmq_queue, "jobs")

        assert message.payload.value == "trace-me"
        assert message.headers[HEADER_CORRELATION_ID] == str(original.correlation_id)
        assert message.headers[HEADER_EXECUTION_ID] == str(original.execution_id)
        assert message.headers[HEADER_TENANT_ID] == str(tenant.tenant_id)
        assert message.headers[HEADER_EVENT_ID] == message.key
        assert message.delivery_count == 1

        # 4. Process under a consumer-side execution context.
        observed: dict[str, Any] = {}

        async def handler(msg: QueueMessage[Any]) -> None:
            observed["metadata"] = ctx.inv_ctx.get_metadata()

        consumer_metadata = InvocationMetadata(
            execution_id=uuid7(),
            correlation_id=uuid7(),  # consumer's own id, must be replaced
        )

        with ctx.inv_ctx.bind_metadata(metadata=consumer_metadata):
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

        # The handler ran under the ORIGINAL correlation id...
        assert metadata.correlation_id == original.correlation_id
        # ...caused by the consumed event.
        assert metadata.causation_id == UUID(message.headers[HEADER_EVENT_ID])

        await rabbitmq_queue.ack("jobs", [message.id])


# ....................... #


@pytest.mark.asyncio
async def test_caller_headers_round_trip_and_reserved_keys_win(
    rabbitmq_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    queue = f"hdrs-{uuid4().hex[:8]}"

    await rabbitmq_queue.enqueue(
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

    message = await _receive_until(rabbitmq_queue, queue)

    assert message.headers["trace"] == "t-1"
    assert message.headers["forze_correlation_id"] == "corr-1"
    # Reserved key surfaced through its own field, not the headers mapping.
    assert message.key == "real-key"
    assert "forze_key" not in message.headers
    assert message.type == "created"

    await rabbitmq_queue.ack(queue, [message.id])


# ....................... #


@pytest.mark.asyncio
async def test_delivery_count_reports_redelivery(
    rabbitmq_queue: RabbitMQQueueAdapter,
    queue_payload_cls,
) -> None:
    queue = f"redeliver-{uuid4().hex[:8]}"

    await rabbitmq_queue.enqueue(queue, queue_payload_cls(value="x"))

    first = await _receive_until(rabbitmq_queue, queue)
    assert first.delivery_count == 1

    # Broker requeue -> redelivered flag -> approximated as 2.
    await rabbitmq_queue.nack(queue, [first.id], requeue=True)

    second = await _receive_until(rabbitmq_queue, queue)
    assert second.delivery_count == 2

    await rabbitmq_queue.ack(queue, [second.id])
