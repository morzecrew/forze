"""Unit tests for :class:`~forze.application.integrations.outbox.OutboxStaging`."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec, StagedOutboxEntry
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.outbox import InvocationOutboxEnricher
from forze.application.integrations.outbox import OutboxStaging
from forze.base.serialization import PydanticModelCodec


class _Payload(BaseModel):
    value: str


@pytest.mark.asyncio
async def test_flush_delegates_buffered_rows() -> None:
    flushed: list[StagedOutboxEntry] = []

    async def _flush(rows: list[StagedOutboxEntry]) -> int:
        flushed.extend(rows)
        return len(rows)

    spec = OutboxSpec(
        name="events",
        codec=PydanticModelCodec(_Payload),
    )
    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())
    coord = OutboxStaging(
        staging=ctx.outbox_staging,
        spec=spec,
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
        flush_rows=_flush,
    )

    await coord.stage("demo.created", _Payload(value="a"))
    count = await coord.flush()

    assert count == 1
    assert len(flushed) == 1
    assert flushed[0].event.event_type == "demo.created"
    assert ctx.outbox_staging.flushed is True


@pytest.mark.asyncio
async def test_cannot_stage_after_flush() -> None:
    spec = OutboxSpec(
        name="events",
        codec=PydanticModelCodec(_Payload),
    )
    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())

    async def _flush(_rows: list[StagedOutboxEntry]) -> int:
        return 0

    coord = OutboxStaging(
        staging=ctx.outbox_staging,
        spec=spec,
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
        flush_rows=_flush,
    )
    await coord.flush()

    with pytest.raises(Exception, match="after flush"):
        await coord.stage("x", _Payload(value="y"))


@pytest.mark.asyncio
async def test_stage_in_new_task_after_flush_does_not_raise() -> None:
    flushed: list[StagedOutboxEntry] = []

    async def _flush(rows: list[StagedOutboxEntry]) -> int:
        flushed.extend(rows)
        return len(rows)

    spec = OutboxSpec(
        name="events",
        codec=PydanticModelCodec(_Payload),
    )
    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())
    coord = OutboxStaging(
        staging=ctx.outbox_staging,
        spec=spec,
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
        flush_rows=_flush,
    )

    # First operation flushes within its own task (e.g. a request task).
    await asyncio.create_task(coord.flush())

    async def _next_operation() -> int:
        await coord.stage("demo.created", _Payload(value="b"))
        return await coord.flush()

    # A new task (e.g. the next request) starts with a fresh flushed flag.
    count = await asyncio.create_task(_next_operation())

    assert count == 1
    assert len(flushed) == 1


@pytest.mark.asyncio
async def test_concurrent_tasks_do_not_share_flushed_state() -> None:
    flushed: list[StagedOutboxEntry] = []

    async def _flush(rows: list[StagedOutboxEntry]) -> int:
        flushed.extend(rows)
        return len(rows)

    spec = OutboxSpec(
        name="events",
        codec=PydanticModelCodec(_Payload),
    )
    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())
    coord = OutboxStaging(
        staging=ctx.outbox_staging,
        spec=spec,
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
        flush_rows=_flush,
    )

    first_flushed = asyncio.Event()

    async def _task_a() -> int:
        await coord.stage("demo.created", _Payload(value="a"))
        count = await coord.flush()
        first_flushed.set()
        return count

    async def _task_b() -> int:
        # Wait until task A has flushed, then stage and flush independently.
        await first_flushed.wait()
        await coord.stage("demo.created", _Payload(value="b"))
        return await coord.flush()

    count_a, count_b = await asyncio.gather(_task_a(), _task_b())

    assert count_a == 1
    assert count_b == 1
    assert len(flushed) == 2
