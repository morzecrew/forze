"""End-to-end outbox payload encryption: ciphertext at rest, plaintext after relay."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.outbox import (
    OutboxDestination,
    OutboxEncryptionTier,
    OutboxSpec,
    OutboxStatus,
)
from forze.application.contracts.queue import QueueMessage, QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.application.integrations.outbox import is_encrypted_payload
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.consumer import QueueConsumer
from forze_kits.integrations.outbox import (
    OutboxRelay,
    outbox_flush_tx_on_success_factory,
)
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.adapters import MockState
from forze_mock.outbox_adapter import MockOutboxRow

# ----------------------- #


class _EventPayload(BaseModel):
    n: int


def _specs(
    *, encryption: OutboxEncryptionTier
) -> tuple[OutboxSpec[_EventPayload], QueueSpec[_EventPayload]]:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
        encryption=encryption,
    )
    return outbox_spec, QueueSpec(name="jobs", codec=codec)


# ....................... #


@pytest.mark.asyncio
async def test_payload_encrypted_at_rest_then_decrypted_on_relay() -> None:
    outbox_spec, queue_spec = _specs(encryption="at_rest")
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()

        await ctx.outbox.command(outbox_spec).stage("job.requested", _EventPayload(n=7))
        await outbox_flush_tx_on_success_factory(outbox_spec)(ctx)(0, 0)

        state = ctx.deps.provide(MockStateDepKey)
        stored = state.outbox_rows["events"][0].payload

        # Ciphertext at rest: the stored payload is an envelope wrapper, not {"n": 7}.
        assert is_encrypted_payload(stored)
        assert "n" not in stored

        result = await OutboxRelay(outbox_spec=outbox_spec).to_queue(ctx, queue_spec)

        # The relay decrypted, decoded, and published the plaintext model.
        assert result.published == 1
        entry = state.queues["jobs"]["jobs"][0]
        assert entry.message.payload == _EventPayload(n=7)


@pytest.mark.asyncio
async def test_relay_tolerates_legacy_plaintext_rows() -> None:
    """A plaintext row written before encryption was enabled still relays."""

    outbox_spec, queue_spec = _specs(encryption="at_rest")
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        state.outbox_rows["events"] = [
            MockOutboxRow(
                id=uuid4(),
                outbox_route="events",
                event_id=uuid4(),
                event_type="job.requested",
                payload={"n": 9},  # legacy plaintext, no envelope wrapper
                status=OutboxStatus.PENDING,
                tenant_id=None,
                execution_id=None,
                correlation_id=None,
                causation_id=None,
                occurred_at=utcnow(),
                created_at=utcnow(),
                processing_at=None,
            )
        ]

        result = await OutboxRelay(outbox_spec=outbox_spec).to_queue(ctx, queue_spec)

        assert result.published == 1
        assert state.queues["jobs"]["jobs"][0].message.payload == _EventPayload(n=9)


@pytest.mark.asyncio
async def test_end_to_end_relay_tolerates_legacy_plaintext_rows() -> None:
    """A plaintext row on an end_to_end route relays as the decoded model, not the raw
    dict (which would mis-route through the transient-retry path)."""

    outbox_spec, queue_spec = _specs(encryption="end_to_end")
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        state.outbox_rows["events"] = [
            MockOutboxRow(
                id=uuid4(),
                outbox_route="events",
                event_id=uuid4(),
                event_type="job.requested",
                payload={"n": 11},  # legacy plaintext, no envelope wrapper
                status=OutboxStatus.PENDING,
                tenant_id=None,
                execution_id=None,
                correlation_id=None,
                causation_id=None,
                occurred_at=utcnow(),
                created_at=utcnow(),
                processing_at=None,
            )
        ]

        result = await OutboxRelay(outbox_spec=outbox_spec).to_queue(ctx, queue_spec)

        assert result.published == 1
        assert state.queues["jobs"]["jobs"][0].message.payload == _EventPayload(n=11)


@pytest.mark.asyncio
async def test_end_to_end_ciphertext_through_broker_decrypted_by_consumer() -> None:
    """e2e: relay publishes ciphertext; the consumer decrypts before the handler."""

    outbox_spec, queue_spec = _specs(encryption="end_to_end")
    inbox_spec = InboxSpec(name="events")
    state = MockState()
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(
            MockDepsModule(state=state, strict_tx=True)
        ).freeze()
    )

    async with runtime.scope():
        ctx = runtime.get_context()

        await ctx.outbox.command(outbox_spec).stage("job.requested", _EventPayload(n=42))
        await outbox_flush_tx_on_success_factory(outbox_spec)(ctx)(0, 0)

        # e2e: the relay forwards ciphertext, it is NOT decrypted before publish.
        result = await OutboxRelay(outbox_spec=outbox_spec).to_queue(ctx, queue_spec)
        assert result.published == 1
        # The broker holds the ciphertext envelope, not the plaintext model.
        assert is_encrypted_payload(state.queues["jobs"]["jobs"][0].message.payload)

        received: list[_EventPayload] = []

        async def _handler(message: QueueMessage[_EventPayload]) -> None:
            received.append(message.payload)

        run = await QueueConsumer(
            queue="jobs",
            queue_spec=queue_spec,
            handler=_handler,
            inbox_spec=inbox_spec,
            tx_route="mock",
        ).run(ctx, timeout=timedelta(milliseconds=250))

        # The consumer decrypted the envelope and the handler saw the plaintext model.
        assert run.processed == 1
        assert received == [_EventPayload(n=42)]


@pytest.mark.asyncio
async def test_plaintext_route_unaffected() -> None:
    """encrypt=False keeps payloads plaintext at rest (no regression)."""

    outbox_spec, queue_spec = _specs(encryption="none")
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        ctx = runtime.get_context()
        await ctx.outbox.command(outbox_spec).stage("job.requested", _EventPayload(n=3))
        await outbox_flush_tx_on_success_factory(outbox_spec)(ctx)(0, 0)

        state = ctx.deps.provide(MockStateDepKey)
        assert state.outbox_rows["events"][0].payload == {"n": 3}
