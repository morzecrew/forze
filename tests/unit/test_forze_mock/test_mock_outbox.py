"""Unit tests for :class:`~forze_mock.outbox_adapter.MockOutboxStore`."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import (
    IntegrationEvent,
    OutboxDestination,
    OutboxSpec,
    OutboxStatus,
)
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import relay_outbox_to_queue
from forze_mock import MockDepsModule, MockStateDepKey
from forze_mock.outbox_adapter import MockOutboxRow


class _EventPayload(BaseModel):
    n: int


@pytest.mark.asyncio
async def test_mock_outbox_flush_and_relay_to_queue() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        outbox = ctx.outbox.command(outbox_spec)
        await outbox.stage("job.requested", _EventPayload(n=1))
        assert await outbox.flush() == 1

        result = await relay_outbox_to_queue(
            ctx,
            outbox_spec=outbox_spec,
            queue_spec=queue_spec,
        )

        assert result.claimed == 1
        assert result.published == 1
        assert result.failed == 0
        assert len(state.queues["jobs"]["jobs"]) == 1
        assert state.queues["jobs"]["jobs"][0].message.payload.n == 1


@pytest.mark.asyncio
async def test_mock_outbox_ordering_key_round_trips_stage_row_claim() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(name="events", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        outbox = ctx.outbox.command(outbox_spec)
        await outbox.stage("job.requested", _EventPayload(n=1), ordering_key="agg-1")
        await outbox.stage("job.updated", _EventPayload(n=2))
        assert await outbox.flush() == 2

        rows = {r.event_type: r for r in state.outbox_rows["events"]}
        assert rows["job.requested"].ordering_key == "agg-1"
        assert rows["job.updated"].ordering_key is None

        claims = {
            c.event_type: c
            for c in await ctx.outbox.query(outbox_spec).claim_pending()
        }
        assert claims["job.requested"].ordering_key == "agg-1"
        assert claims["job.updated"].ordering_key is None


@pytest.mark.asyncio
async def test_mock_outbox_duplicate_event_id_skips_second_flush() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(name="events", codec=codec)
    event_id = uuid4()

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    state_holder: list = []

    async with runtime.scope():
        ctx = runtime.get_context()
        state_holder.append(ctx.deps.provide(MockStateDepKey))
        outbox = ctx.outbox.command(outbox_spec)
        await outbox.stage_event(
            IntegrationEvent(
                event_type="job.requested",
                payload=_EventPayload(n=1),
                event_id=event_id,
            )
        )
        assert await outbox.flush() == 1

    async with runtime.scope():
        ctx = runtime.get_context()
        outbox = ctx.outbox.command(outbox_spec)
        await outbox.stage_event(
            IntegrationEvent(
                event_type="job.requested",
                payload=_EventPayload(n=2),
                event_id=event_id,
            )
        )
        assert await outbox.flush() == 0

    state = state_holder[0]
    assert len(state.outbox_rows["events"]) == 1
    assert state.outbox_rows["events"][0].payload["n"] == 1


@pytest.mark.asyncio
async def test_mock_outbox_reclaim_stale_processing() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(name="events", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        row_id = uuid4()
        state.outbox_rows["events"] = [
            MockOutboxRow(
                id=row_id,
                outbox_route="events",
                event_id=uuid4(),
                event_type="job.requested",
                payload={"n": 1},
                status=OutboxStatus.PROCESSING,
                tenant_id=None,
                execution_id=None,
                correlation_id=None,
                causation_id=None,
                occurred_at=utcnow(),
                created_at=utcnow(),
                processing_at=utcnow() - timedelta(hours=1),
            )
        ]
        query = ctx.outbox.query(outbox_spec)
        reclaimed = await query.reclaim_stale_processing(
            older_than=utcnow() - timedelta(minutes=5),
        )

        assert reclaimed == 1
        assert state.outbox_rows["events"][0].status == OutboxStatus.PENDING
        assert state.outbox_rows["events"][0].processing_at is None


@pytest.mark.asyncio
async def test_mock_outbox_requeue_failed() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(name="events", codec=codec)
    row_id = uuid4()

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        state = ctx.deps.provide(MockStateDepKey)
        state.outbox_rows["events"] = [
            MockOutboxRow(
                id=row_id,
                outbox_route="events",
                event_id=uuid4(),
                event_type="job.requested",
                payload={"n": 1},
                status=OutboxStatus.FAILED,
                tenant_id=None,
                execution_id=None,
                correlation_id=None,
                causation_id=None,
                occurred_at=utcnow(),
                created_at=utcnow(),
                last_error="err",
            )
        ]
        query = ctx.outbox.query(outbox_spec)
        updated = await query.requeue_failed([row_id])

        assert updated == 1
        row = state.outbox_rows["events"][0]
        assert row.status == OutboxStatus.PENDING
        assert row.last_error is None
