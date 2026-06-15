"""Unit tests for :class:`~forze.application.integrations.outbox.OutboxStaging`."""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec, StagedOutboxEntry
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.application.execution.outbox import InvocationOutboxEnricher
from forze.application.integrations.outbox import OutboxStaging
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization import PydanticModelCodec


class _Payload(BaseModel):
    value: str


def _coord(
    ctx: ExecutionContext,
    name: str,
    sink: list[StagedOutboxEntry],
) -> OutboxStaging[_Payload]:
    async def _flush(rows: list[StagedOutboxEntry]) -> int:
        sink.extend(rows)
        return len(rows)

    spec = OutboxSpec(name=name, codec=PydanticModelCodec(_Payload))
    return OutboxStaging(
        staging=ctx.outbox_staging,
        spec=spec,
        enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
        flush_rows=_flush,
    )


def test_construction_without_cipher_for_encrypting_spec_fails_closed() -> None:
    """A route that declares encryption but is wired without a keyring is refused at
    construction, not silently staged as plaintext."""

    async def _flush(rows: list[StagedOutboxEntry]) -> int:
        return 0  # never reached — construction raises first

    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())
    spec = OutboxSpec(
        name="events", codec=PydanticModelCodec(_Payload), encryption="at_rest"
    )

    with pytest.raises(CoreException) as ei:
        OutboxStaging(
            staging=ctx.outbox_staging,
            spec=spec,
            enricher=InvocationOutboxEnricher(inv=ctx.inv_ctx),
            flush_rows=_flush,
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert ei.value.code == "core.outbox.payload_cipher_missing"


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
    assert ctx.outbox_staging.flushed_for("events") is True


@pytest.mark.asyncio
async def test_stage_threads_ordering_key_onto_event() -> None:
    """`stage(..., ordering_key=...)` reaches the flushed entry's event verbatim."""

    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())
    flushed: list[StagedOutboxEntry] = []
    coord = _coord(ctx, "events", flushed)

    await coord.stage("demo.created", _Payload(value="keyed"), ordering_key="agg-7")
    await coord.stage("demo.updated", _Payload(value="unkeyed"))
    assert await coord.flush() == 2

    by_type = {e.event.event_type: e.event for e in flushed}
    assert by_type["demo.created"].ordering_key == "agg-7"
    # Default stays None: relay falls back to key=str(event_id).
    assert by_type["demo.updated"].ordering_key is None


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


@pytest.mark.asyncio
async def test_two_specs_flush_only_their_own_rows() -> None:
    """Regression: two outbox specs in one operation must not share a buffer."""

    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())
    flushed_a: list[StagedOutboxEntry] = []
    flushed_b: list[StagedOutboxEntry] = []
    coord_a = _coord(ctx, "route_a", flushed_a)
    coord_b = _coord(ctx, "route_b", flushed_b)

    await coord_a.stage("a.created", _Payload(value="a1"))
    await coord_b.stage("b.created", _Payload(value="b1"))
    await coord_a.stage("a.updated", _Payload(value="a2"))

    # First flush (route A) must NOT hand route B's rows to A's port.
    count_a = await coord_a.flush()
    count_b = await coord_b.flush()

    assert count_a == 2
    assert count_b == 1
    assert {e.event.event_type for e in flushed_a} == {"a.created", "a.updated"}
    assert all(e.outbox_route == "route_a" for e in flushed_a)
    assert [e.event.event_type for e in flushed_b] == ["b.created"]
    assert all(e.outbox_route == "route_b" for e in flushed_b)


@pytest.mark.asyncio
async def test_flush_then_stage_on_other_route_unaffected() -> None:
    """Flushing route A neither blocks staging on route B nor drops B's rows."""

    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())
    flushed_a: list[StagedOutboxEntry] = []
    flushed_b: list[StagedOutboxEntry] = []
    coord_a = _coord(ctx, "route_a", flushed_a)
    coord_b = _coord(ctx, "route_b", flushed_b)

    await coord_a.stage("a.created", _Payload(value="a1"))
    assert await coord_a.flush() == 1

    # Route A is flushed for this task; route B must still accept staging.
    await coord_b.stage("b.created", _Payload(value="b1"))
    assert await coord_b.flush() == 1
    assert [e.event.event_type for e in flushed_b] == ["b.created"]

    # Route A itself stays closed for this task.
    with pytest.raises(Exception, match="after flush"):
        await coord_a.stage("a.late", _Payload(value="a2"))


@pytest.mark.asyncio
async def test_concurrent_tasks_same_route_have_isolated_buffers() -> None:
    """Two concurrent tasks staging on the same route never see each other's rows."""

    ctx = ExecutionContext(deps=DepsRegistry().freeze().resolve())
    flushed: list[StagedOutboxEntry] = []
    coord = _coord(ctx, "events", flushed)

    a_staged = asyncio.Event()
    b_flushed = asyncio.Event()

    async def _task_a() -> int:
        await coord.stage("demo.created", _Payload(value="a"))
        a_staged.set()
        # Hold the staged row while task B stages and flushes its own.
        await b_flushed.wait()
        return await coord.flush()

    async def _task_b() -> int:
        await a_staged.wait()
        await coord.stage("demo.created", _Payload(value="b"))
        count = await coord.flush()
        b_flushed.set()
        return count

    count_a, count_b = await asyncio.gather(_task_a(), _task_b())

    # Each task flushed exactly its own row, despite sharing the route.
    assert count_a == 1
    assert count_b == 1
    values = sorted(e.event.payload.value for e in flushed)
    assert values == ["a", "b"]
