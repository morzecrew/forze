"""Unit tests for :meth:`~forze_kits.integrations.outbox.OutboxRelay.to_queue`."""

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
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import OutboxRelay
from forze_mock import MockDepsModule, MockRouteConfig, MockStateDepKey
from forze_mock.adapters.outbox import MockOutboxRow


class _EventPayload(BaseModel):
    n: int


@pytest.mark.asyncio
async def test_relay_of_tenant_aware_outbox_unbound_fails_with_actionable_error() -> None:
    # A tenant-aware outbox can't be read by the tenant-less background relay; the opaque
    # tenant_required is rewrapped into an actionable error naming the tenant-global contract.
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    module = MockDepsModule(routes={"events": MockRouteConfig(tenant_aware=True)})
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()  # no tenant bound — the background-relay case
        with pytest.raises(CoreException) as caught:
            await OutboxRelay(outbox_spec=outbox_spec).to_queue(ctx, queue_spec)

    assert caught.value.code == "outbox_relay_tenant_unbound"
    assert "tenant-global" in caught.value.summary


@pytest.mark.asyncio
async def test_relay_reclaims_stale_processing_before_publish() -> None:
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
        state.outbox_rows["events"] = [
            MockOutboxRow(
                id=uuid4(),
                outbox_route="events",
                event_id=uuid4(),
                event_type="job.requested",
                payload={"n": 7},
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

        result = await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=timedelta(minutes=5)
        ).to_queue(ctx, queue_spec)

        assert result.reclaimed >= 1
        assert result.published == 1
        assert len(state.queues["jobs"]["jobs"]) == 1


@pytest.mark.asyncio
async def test_relay_marks_invalid_payload_failed() -> None:
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
        row_id = uuid4()
        state.outbox_rows["events"] = [
            MockOutboxRow(
                id=row_id,
                outbox_route="events",
                event_id=uuid4(),
                event_type="job.requested",
                payload={"not_n": "bad"},
                status=OutboxStatus.PENDING,
                tenant_id=None,
                execution_id=None,
                correlation_id=None,
                causation_id=None,
                occurred_at=utcnow(),
                created_at=utcnow(),
            )
        ]

        result = await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=None
        ).to_queue(ctx, queue_spec)

        assert result.failed == 1
        assert result.published == 0
        assert state.queues.get("jobs", {}).get("jobs", []) == []
        row = next(r for r in state.outbox_rows["events"] if r.id == row_id)
        assert row.status == OutboxStatus.FAILED
        assert row.last_error is not None


@pytest.mark.asyncio
async def test_relay_twice_does_not_duplicate_queue_messages() -> None:
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
        await ctx.outbox.command(outbox_spec).stage("job.requested", _EventPayload(n=1))
        await ctx.outbox.command(outbox_spec).flush()

        first = await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=timedelta(hours=1)
        ).to_queue(ctx, queue_spec)
        second = await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=timedelta(hours=1)
        ).to_queue(ctx, queue_spec)

        assert first.published == 1
        assert second.claimed == 0
        assert second.published == 0
        assert len(state.queues["jobs"]["jobs"]) == 1


@pytest.mark.asyncio
async def test_relay_enqueue_uses_event_id_as_queue_key() -> None:
    codec = PydanticModelCodec(_EventPayload)
    event_id = uuid4()
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
        await ctx.outbox.command(outbox_spec).stage_event(
            IntegrationEvent(
                event_type="job.requested",
                payload=_EventPayload(n=9),
                event_id=event_id,
            )
        )
        await ctx.outbox.command(outbox_spec).flush()

        await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=None
        ).to_queue(ctx, queue_spec)

        message = state.queues["jobs"]["jobs"][0].message
        assert message.key == str(event_id)
