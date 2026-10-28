"""Runtime evaluation of paired laws, exact decimal sums, and scopes with no rows.

A law over two read-sets per scope (lines bounded by their order), a decimal sum compared without
float rounding (``0.1 + 0.2 <= 0.3``), and at least one line per order — the scope the grouped
oracle never sees unless the order side names it. ``evaluate`` reads both sides for one binding.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.invariants import Measure, ReadSet, SumOf, SystemInvariant
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze_kits.invariants import enforce_preventive, evaluate
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps
from tests.support.paired_invariants import (
    LINES,
    LINES_WITHIN_ORDER_QTY,
    LINES_WITHIN_ORDER_TOTAL,
    NOTES,
    ORDER_HAS_A_LINE,
    ORDER_HAS_A_NOTE,
    ORDERS,
    REFUNDS_WITHIN_SALES,
    LineCreate,
    NoteCreate,
    OrderCreate,
)

# ----------------------- #

_SER = IsolationLevel.SERIALIZABLE


def _ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


async def _order(ctx: ExecutionContext, *, qty: int = 0, total: str = "0") -> UUID:
    order = await ctx.document.command(ORDERS).create(OrderCreate(qty=qty, total=Decimal(total)))
    return order.id


async def _line(
    ctx: ExecutionContext,
    order_id: UUID,
    *,
    qty: int = 0,
    amount: str = "0",
    kind: str = "sale",
) -> UUID:
    line = await ctx.document.command(LINES).create(
        LineCreate(order_id=order_id, qty=qty, amount=Decimal(amount), kind=kind)
    )
    return line.id


# ----------------------- #


class TestPairedLaw:
    async def test_lines_within_the_order_hold_and_report_both_sides(self) -> None:
        ctx = _ctx()
        order = await _order(ctx, qty=3)
        await _line(ctx, order, qty=1)
        await _line(ctx, order, qty=2)

        result = await evaluate(LINES_WITHIN_ORDER_QTY, ctx, {"order_id": order})

        assert result.held
        assert (result.observed, result.against) == (3.0, 3.0)

    async def test_an_over_allocation_is_caught(self) -> None:
        ctx = _ctx()
        order = await _order(ctx, qty=3)
        other = await _order(ctx, qty=10)
        await _line(ctx, order, qty=2)
        await _line(ctx, order, qty=2)
        await _line(ctx, other, qty=9)  # another order's lines never count against this one

        result = await evaluate(LINES_WITHIN_ORDER_QTY, ctx, {"order_id": order})

        assert not result.held
        assert (result.observed, result.against) == (4.0, 3.0)

    async def test_a_sum_bounded_by_another_sum(self) -> None:
        ctx = _ctx()
        order = await _order(ctx)
        await _line(ctx, order, amount="5", kind="sale")
        await _line(ctx, order, amount="6", kind="refund")

        result = await evaluate(REFUNDS_WITHIN_SALES, ctx, {"order_id": order})

        assert not result.held
        assert (result.observed, result.against) == (Decimal(6), Decimal(5))

    async def test_preventive_enforcement_rolls_back_an_over_allocation(self) -> None:
        ctx = _ctx()
        order = await _order(ctx, qty=3)

        with pytest.raises(exc, match="aggregate observed 4.0 against 3.0"):
            async with ctx.tx_ctx.scope("mock", isolation=_SER):
                await _line(ctx, order, qty=4)
                await enforce_preventive(LINES_WITHIN_ORDER_QTY, ctx, {"order_id": order})

        assert (await evaluate(LINES_WITHIN_ORDER_QTY, ctx, {"order_id": order})).observed == 0

    async def test_a_missing_scope_key_fails_closed(self) -> None:
        with pytest.raises(exc, match="missing scope key"):
            await evaluate(LINES_WITHIN_ORDER_QTY, _ctx(), {"id": uuid4()})


class TestExactDecimals:
    async def test_tenths_sum_exactly_to_their_bound(self) -> None:
        # In float, 0.1 + 0.2 is 0.30000000000000004 and fails ``<= 0.3``.
        ctx = _ctx()
        order = await _order(ctx, total="0.3")
        await _line(ctx, order, amount="0.1")
        await _line(ctx, order, amount="0.2")

        result = await evaluate(LINES_WITHIN_ORDER_TOTAL, ctx, {"order_id": order})

        assert result.held
        assert result.observed == Decimal("0.3")
        assert isinstance(result.observed, Decimal)
        assert isinstance(result.against, Decimal)

    async def test_a_hundredth_over_is_caught(self) -> None:
        ctx = _ctx()
        order = await _order(ctx, total="0.3")
        await _line(ctx, order, amount="0.1")
        await _line(ctx, order, amount="0.21")

        assert not (await evaluate(LINES_WITHIN_ORDER_TOTAL, ctx, {"order_id": order})).held

    async def test_a_cent_beyond_float_precision_is_caught(self) -> None:
        # float(10**16 + 0.01) == float(10**16): a total rounded to float would hide the overrun.
        ctx = _ctx()
        order = await _order(ctx, total="10000000000000000")
        await _line(ctx, order, amount="10000000000000000")
        await _line(ctx, order, amount="0.01")

        assert not (await evaluate(LINES_WITHIN_ORDER_TOTAL, ctx, {"order_id": order})).held

    async def test_a_single_law_receives_a_decimal(self) -> None:
        ctx = _ctx()
        order = await _order(ctx)
        await _line(ctx, order, amount="0.1")
        await _line(ctx, order, amount="0.2")
        law = SystemInvariant(
            name="exactly_three_tenths",
            read_set=ReadSet(spec=LINES, scope_keys=("order_id",)),
            aggregate=SumOf("amount"),
            holds=lambda total: total == Decimal("0.3"),
        )

        assert (await evaluate(law, ctx, {"order_id": order})).held


class TestScopeWithNoRows:
    async def test_an_order_with_no_lines_is_a_violation(self) -> None:
        ctx = _ctx()
        order = await _order(ctx)

        result = await evaluate(ORDER_HAS_A_LINE, ctx, {"order_id": order})

        assert not result.held
        assert (result.observed, result.against) == (0, 1)

    async def test_an_order_with_a_line_holds(self) -> None:
        ctx = _ctx()
        order = await _order(ctx)
        await _line(ctx, order)

        assert (await evaluate(ORDER_HAS_A_LINE, ctx, {"order_id": order})).held

    async def test_removing_the_last_line_is_rolled_back(self) -> None:
        ctx = _ctx()
        order = await _order(ctx)
        line = await _line(ctx, order)

        with pytest.raises(exc, match="would be violated"):
            async with ctx.tx_ctx.scope("mock", isolation=_SER):
                await ctx.document.command(LINES).kill(line)
                await enforce_preventive(ORDER_HAS_A_LINE, ctx, {"order_id": order})

        assert (await evaluate(ORDER_HAS_A_LINE, ctx, {"order_id": order})).observed == 1

    async def test_a_binding_with_rows_on_neither_side_is_not_checked(self) -> None:
        # The oracles never see such a scope, so the runtime does not call holds for it either; an
        # order with qty 0 has a row, so it is checked even though both sums are 0.
        never = SystemInvariant(
            name="never",
            read_set=ReadSet(spec=LINES, scope_keys=("order_id",)),
            aggregate=SumOf("qty"),
            against=Measure(
                read_set=ReadSet(spec=ORDERS, scope_keys=("id",)), aggregate=SumOf("qty")
            ),
            holds=lambda _lines, _order: False,
        )
        ctx = _ctx()
        order = await _order(ctx)

        assert (await evaluate(never, ctx, {"order_id": uuid4()})).held
        assert not (await evaluate(never, ctx, {"order_id": order})).held

    async def test_a_string_reference_binds_the_uuid_id(self) -> None:
        ctx = _ctx()
        noted, bare = await _order(ctx), await _order(ctx)
        await ctx.document.command(NOTES).create(NoteCreate(order_ref=str(noted)))

        result = await evaluate(ORDER_HAS_A_NOTE, ctx, {"order_ref": str(noted)})

        assert (result.held, result.observed, result.against) == (True, 1, 1)
        assert not (await evaluate(ORDER_HAS_A_NOTE, ctx, {"order_ref": str(bare)})).held
