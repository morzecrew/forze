"""Outbox staging follows the transaction scope, not the task (#785).

A root transaction opens a fresh staging scope on every route, so one task can run unit
of work after unit of work (a worker loop, an after-commit effect, an in-process ASGI
client). Inside one root the flushed guard still holds, savepoints included, and a scope
that rolls back takes the events it staged — and its flush — with it.
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import BaseModel

from forze.application.contracts.outbox import OutboxSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Payload(BaseModel):
    note: str


SPEC = OutboxSpec(name="orders-events", codec=PydanticModelCodec(_Payload))


class _Boom(RuntimeError):
    pass


def _ctx(*, strict_tx: bool = True) -> tuple[ExecutionContext, MockState]:
    state = MockState()

    return context_from_modules(MockDepsModule(state=state, strict_tx=strict_tx)), state


def _notes(state: MockState) -> list[str]:
    return [row.payload["note"] for row in state.outbox_rows.get("orders-events", [])]


async def _stage(ctx: ExecutionContext, note: str) -> None:
    await ctx.outbox.command(SPEC).stage("order.placed", _Payload(note=note))


async def _flush(ctx: ExecutionContext) -> int:
    return await ctx.outbox.command(SPEC).flush()


# ....................... #


async def test_each_root_transaction_in_one_task_can_stage_and_flush() -> None:
    ctx, state = _ctx()

    for note in ("first", "second"):
        async with ctx.tx_ctx.scope("mock"):
            await _stage(ctx, note)
            assert await _flush(ctx) == 1

    assert _notes(state) == ["first", "second"]


async def test_staging_after_flush_in_the_same_root_is_refused() -> None:
    ctx, _ = _ctx()

    async with ctx.tx_ctx.scope("mock"):
        await _stage(ctx, "first")
        await _flush(ctx)

        with pytest.raises(CoreException, match="after flush"):
            await _stage(ctx, "late")


async def test_a_savepoint_does_not_reopen_a_flushed_route() -> None:
    ctx, _ = _ctx()

    async with ctx.tx_ctx.scope("mock"):
        await _stage(ctx, "first")
        await _flush(ctx)

        async with ctx.tx_ctx.scope("mock"):
            with pytest.raises(CoreException, match="after flush"):
                await _stage(ctx, "late")


async def test_a_flush_in_a_committed_savepoint_closes_the_route_for_the_root() -> None:
    ctx, _ = _ctx()

    async with ctx.tx_ctx.scope("mock"):
        async with ctx.tx_ctx.scope("mock"):
            await _stage(ctx, "inner")
            await _flush(ctx)

        with pytest.raises(CoreException, match="after flush"):
            await _stage(ctx, "late")


async def test_a_rolled_back_root_discards_what_it_staged() -> None:
    ctx, state = _ctx()

    with pytest.raises(_Boom):
        async with ctx.tx_ctx.scope("mock"):
            await _stage(ctx, "rolled-back")
            raise _Boom

    assert ctx.outbox_staging.peek() == []

    async with ctx.tx_ctx.scope("mock"):
        await _stage(ctx, "kept")
        assert await _flush(ctx) == 1

    assert _notes(state) == ["kept"]


async def test_a_rolled_back_savepoint_discards_its_staging_and_its_flush() -> None:
    ctx, state = _ctx()

    async with ctx.tx_ctx.scope("mock"):
        await _stage(ctx, "outer")

        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                await _stage(ctx, "inner")
                await _flush(ctx)
                raise _Boom

        # The savepoint's rows rolled back, so the root still owns "outer" and may flush it.
        assert await _flush(ctx) == 1

    assert _notes(state) == ["outer"]


async def test_a_savepoint_rollback_the_journal_mock_cannot_undo_writes_the_event_once() -> None:
    # The default mock keeps the savepoint's rows, so the root's flush of the restored
    # "outer" leans on the store's dedup by event id.
    ctx, state = _ctx(strict_tx=False)

    async with ctx.tx_ctx.scope("mock"):
        await _stage(ctx, "outer")

        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                await _stage(ctx, "inner")
                await _flush(ctx)
                raise _Boom

        await _flush(ctx)

    assert _notes(state).count("outer") == 1


async def test_a_sibling_task_never_flushes_what_a_rolled_back_savepoint_staged() -> None:
    ctx, state = _ctx()
    savepoint_done = asyncio.Event()

    async def _fail_in_a_savepoint() -> None:
        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                await _stage(ctx, "inner")
                raise _Boom

        savepoint_done.set()

    async def _flush_after_it() -> int:
        await savepoint_done.wait()

        return await _flush(ctx)

    async with ctx.tx_ctx.scope("mock"):
        # Staged before the gather, so both children start from a non-empty buffer.
        await _stage(ctx, "outer")
        await asyncio.gather(_fail_in_a_savepoint(), _flush_after_it())

    assert _notes(state) == ["outer"]


async def test_a_rollback_in_one_task_keeps_what_a_sibling_staged_meanwhile() -> None:
    ctx, state = _ctx()
    savepoint_entered = asyncio.Event()
    sibling_staged = asyncio.Event()
    rolled_back = asyncio.Event()

    async def _roll_back() -> None:
        with pytest.raises(_Boom):
            async with ctx.tx_ctx.scope("mock"):
                savepoint_entered.set()
                await sibling_staged.wait()
                await _stage(ctx, "rolled-back")
                raise _Boom

        rolled_back.set()

    async def _stage_then_flush() -> None:
        await savepoint_entered.wait()
        await _stage(ctx, "sibling")
        sibling_staged.set()
        await rolled_back.wait()
        await _flush(ctx)

    async with ctx.tx_ctx.scope("mock"):
        # Both children inherit this non-empty buffer; the sibling stages after the
        # savepoint took its snapshot, and must still flush its own entry.
        await _stage(ctx, "outer")
        await asyncio.gather(_roll_back(), _stage_then_flush())

    assert sorted(_notes(state)) == ["outer", "sibling"]


async def test_what_a_child_task_stages_stays_in_that_task() -> None:
    # A child begins with a copy of the parent's staging; its own stages never reach the
    # parent, so a child that stages must flush them itself.
    ctx, _ = _ctx()

    async def _child() -> None:
        await _stage(ctx, "child")

    async with ctx.tx_ctx.scope("mock"):
        await _stage(ctx, "outer")
        await asyncio.create_task(_child())

        assert [entry.event.payload.note for entry in ctx.outbox_staging.peek()] == ["outer"]


async def test_events_staged_before_a_rolled_back_root_survive_it() -> None:
    ctx, state = _ctx()
    await _stage(ctx, "before")

    with pytest.raises(_Boom):
        async with ctx.tx_ctx.scope("mock"):
            await _stage(ctx, "inside")
            await _flush(ctx)
            raise _Boom

    async with ctx.tx_ctx.scope("mock"):
        assert await _flush(ctx) == 1

    assert _notes(state) == ["before"]


async def test_an_after_commit_effect_can_stage_in_its_own_transaction() -> None:
    ctx, state = _ctx()

    async def _effect() -> None:
        async with ctx.tx_ctx.scope("mock"):
            await _stage(ctx, "effect")
            await _flush(ctx)

    async with ctx.tx_ctx.scope("mock"):
        await _stage(ctx, "operation")
        await _flush(ctx)
        await ctx.tx_ctx.run_or_defer(_effect, fatal=True)

    assert _notes(state) == ["operation", "effect"]


async def test_staging_outside_a_transaction_after_its_flush_is_still_refused() -> None:
    # Nothing would flush it: the unit of work that flushed is over, and no new one began.
    ctx, _ = _ctx()

    async with ctx.tx_ctx.scope("mock"):
        await _stage(ctx, "first")
        await _flush(ctx)

    with pytest.raises(CoreException, match="after flush"):
        await _stage(ctx, "late")
