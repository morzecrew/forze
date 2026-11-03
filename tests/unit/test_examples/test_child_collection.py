"""Child collection recipe — the hand-written diff and ``ChildCollectionSync`` agree."""

from __future__ import annotations

from uuid import UUID

import pytest

from examples.recipes.child_collection.app import (
    LINE_SPEC,
    LINES,
    ORDER_SPEC,
    REGISTRY,
    SAVE_LINES,
    CreateOrder,
    LineIn,
    SaveLines,
    edit_an_order,
    save_lines_by_hand,
    to_commands,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_mock import MockDepsModule

# ----------------------- #

FIRST = [LineIn(sku="bolt", quantity=10), LineIn(sku="nut", quantity=10)]
SECOND = [LineIn(sku="nut", quantity=12), LineIn(sku="screw", quantity=1)]


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule())


async def _lines(ctx: ExecutionContext, order_id: UUID) -> dict[str, tuple[int, int, int]]:
    page = await ctx.document.query(LINE_SPEC).find_many(
        filters={"$values": {"order_id": order_id}}
    )

    return {row.sku: (row.quantity, row.position, row.rev) for row in page.hits}


async def test_the_scenario_drops_adds_and_moves() -> None:
    lines = await edit_an_order(context_from_modules(MockDepsModule()))

    assert lines == {"bolt": (10, 0), "washer": (5, 1), "screw": (20, 2)}


async def test_both_versions_leave_the_same_lines(ctx: ExecutionContext) -> None:
    orders = ctx.document.command(ORDER_SPEC)
    by_hand = await orders.create(CreateOrder(customer="a"))
    synced = await orders.create(CreateOrder(customer="b"))

    for lines in (FIRST, SECOND):
        async with ctx.tx_ctx.scope("mock"):
            await save_lines_by_hand(
                ctx, SaveLines(order_id=by_hand.id, rev=by_hand.rev, lines=lines)
            )
            await LINES.reconcile(
                ctx,
                synced.id,
                to_commands(SaveLines(order_id=synced.id, rev=synced.rev, lines=lines)),
            )

    assert await _lines(ctx, by_hand.id) == await _lines(ctx, synced.id)


async def test_a_save_from_a_stale_order_writes_no_line(ctx: ExecutionContext) -> None:
    order = await ctx.document.command(ORDER_SPEC).create(CreateOrder(customer="a"))
    save = REGISTRY.resolve(SAVE_LINES, ctx)
    await save(SaveLines(order_id=order.id, rev=order.rev, lines=FIRST))

    with pytest.raises(CoreException) as ei:
        await save(SaveLines(order_id=order.id, rev=order.rev, lines=SECOND))

    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert set(await _lines(ctx, order.id)) == {"bolt", "nut"}
