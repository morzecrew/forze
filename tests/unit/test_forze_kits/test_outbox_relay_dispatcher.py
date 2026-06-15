"""Unit tests for :meth:`~forze_kits.integrations.outbox.OutboxRelay.run` dispatch."""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxDestination, OutboxSpec
from forze.application.contracts.queue import QueueSpec
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.outbox import OutboxRelay
from forze_mock import MockDepsModule, MockStateDepKey


class _EventPayload(BaseModel):
    n: int


@pytest.mark.asyncio
async def test_relay_outbox_dispatches_queue() -> None:
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

        result = await OutboxRelay(
            outbox_spec=outbox_spec, reclaim_stale_after=None
        ).run(ctx, queue_spec=queue_spec)

        assert result.published == 1
        assert len(state.queues["jobs"]["jobs"]) == 1


@pytest.mark.asyncio
async def test_relay_outbox_queue_kind_without_queue_spec_raises() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.queue(route="jobs", channel="jobs"),
    )

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(Exception, match="queue_spec is required"):
            await OutboxRelay(
                outbox_spec=outbox_spec, reclaim_stale_after=None
            ).run(ctx)


@pytest.mark.asyncio
async def test_relay_outbox_stream_kind_with_queue_spec_only_raises() -> None:
    codec = PydanticModelCodec(_EventPayload)
    outbox_spec = OutboxSpec(
        name="events",
        codec=codec,
        destination=OutboxDestination.stream(route="audit", channel="audit"),
    )
    queue_spec = QueueSpec(name="jobs", codec=codec)

    module = MockDepsModule()
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(Exception, match="stream_spec is required"):
            await OutboxRelay(
                outbox_spec=outbox_spec, reclaim_stale_after=None
            ).run(ctx, queue_spec=queue_spec)
