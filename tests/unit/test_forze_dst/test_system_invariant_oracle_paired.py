"""compile_oracle over paired laws, exact decimal sums, and scopes with no rows — both oracles.

The final-state oracle runs one grouped aggregate per side and checks every scope present on either
side, so an order with no lines is checked once the order side names it. The per-commit fold
materializes both sides' routes from the value-trace and sums a ``Decimal`` exactly from the row's
native copy. Each law here is the one the runtime tests evaluate, so the evaluators agree.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest

from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
from forze_dst import Cluster, SimulationConfig
from forze_dst.cluster import ClusterConfig
from forze_dst.invariants import check, compile_oracle
from forze_dst.oracle.recorder import Event, History, Recorder, bind_recorder
from forze_dst.oracle.system_invariants import SYSTEM_INVARIANT_KIND
from forze_mock import MockDepsModule
from forze_mock.state import MockState
from tests.support.execution_context import context_from_deps
from tests.support.paired_invariants import (
    LINES,
    LINES_WITHIN_ORDER_QTY,
    LINES_WITHIN_ORDER_TOTAL,
    ORDER_HAS_A_LINE,
    ORDERS,
    REFUNDS_WITHIN_SALES,
    LineCreate,
    OrderCreate,
)

# ----------------------- #
# Final state, driven directly against the mock with a bound recorder.


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


async def _final_state(law: Any, ctx: ExecutionContext) -> tuple[History, list[Any]]:
    oracle = compile_oracle(law)
    recorder = Recorder(seed=0)

    with bind_recorder(recorder):
        await oracle.observe(ctx)

    return recorder.history, check(recorder.history, list(oracle.invariants))


def _checked(history: History) -> dict[Any, Any]:
    return {
        event.fields["scope"]["order_id"]: (event.fields["observed"], event.fields["against"])
        for event in history.of_kind(SYSTEM_INVARIANT_KIND)
    }


class TestFinalStatePairedLaw:
    async def test_flags_only_the_over_allocated_order_and_checks_every_order(self) -> None:
        ctx = _ctx()
        within = await _order(ctx, qty=3)
        over = await _order(ctx, qty=3)
        empty = await _order(ctx, qty=5)
        await _line(ctx, within, qty=1)
        await _line(ctx, within, qty=2)
        await _line(ctx, over, qty=2)
        await _line(ctx, over, qty=2)

        history, violations = await _final_state(LINES_WITHIN_ORDER_QTY, ctx)

        assert _checked(history) == {within: (3.0, 3.0), over: (4.0, 3.0), empty: (0.0, 5.0)}
        assert len(violations) == 1
        assert str(over) in violations[0].message
        assert "aggregate observed 4.0 against 3.0" in violations[0].message

    async def test_lines_of_a_missing_order_are_checked_against_zero(self) -> None:
        ctx = _ctx()
        await _line(ctx, UUID(int=7), qty=1)

        history, violations = await _final_state(LINES_WITHIN_ORDER_QTY, ctx)

        assert _checked(history) == {UUID(int=7): (1.0, 0.0)}
        assert len(violations) == 1

    async def test_a_sum_bounded_by_another_sum_over_one_collection(self) -> None:
        ctx = _ctx()
        order = await _order(ctx)
        await _line(ctx, order, amount="5", kind="sale")
        await _line(ctx, order, amount="6", kind="refund")

        _, violations = await _final_state(REFUNDS_WITHIN_SALES, ctx)

        assert len(violations) == 1
        assert "against 5" in violations[0].message


class TestFinalStateExactDecimals:
    async def test_tenths_sum_exactly_and_are_recorded_as_decimals(self) -> None:
        ctx = _ctx()
        order = await _order(ctx, total="0.3")
        await _line(ctx, order, amount="0.1")
        await _line(ctx, order, amount="0.2")

        history, violations = await _final_state(LINES_WITHIN_ORDER_TOTAL, ctx)

        assert violations == []
        observed, against = _checked(history)[order]
        assert (observed, against) == (Decimal("0.3"), Decimal("0.3"))
        assert isinstance(observed, Decimal)

    async def test_a_cent_beyond_float_precision_is_caught(self) -> None:
        ctx = _ctx()
        order = await _order(ctx, total="10000000000000000")
        await _line(ctx, order, amount="10000000000000000")
        await _line(ctx, order, amount="0.01")

        _, violations = await _final_state(LINES_WITHIN_ORDER_TOTAL, ctx)

        assert len(violations) == 1


class TestFinalStateScopeWithNoRows:
    async def test_flags_the_order_with_no_lines(self) -> None:
        ctx = _ctx()
        with_line = await _order(ctx)
        without = await _order(ctx)
        await _line(ctx, with_line)

        history, violations = await _final_state(ORDER_HAS_A_LINE, ctx)

        assert _checked(history) == {with_line: (1, 1), without: (0, 1)}
        assert len(violations) == 1
        assert str(without) in violations[0].message


# ----------------------- #
# Per commit, over a synthesized value-trace spanning both routes.

_seq = iter(range(1, 10_000))


def _event(fields: dict[str, Any]) -> Event:
    seq = next(_seq)
    return Event(seq=seq, kind="trace", at=float(seq), fields={**fields, "trace_seq": seq})


def _write(tx_id: int, route: str, row: dict[str, Any], *, native: bool = True) -> Event:
    """A captured write: the JSON ``result`` (decimals as strings) and by default the native row."""

    json_row = {key: str(v) if isinstance(v, Decimal) else v for key, v in row.items()}
    fields: dict[str, Any] = {
        "trace_domain": "document",
        "op": "update",
        "route": route,
        "phase": "command",
        "tx_id": tx_id,
        "result": json_row,
    }

    if native:
        fields["result_native"] = row

    return _event(fields)


def _kill(tx_id: int, route: str, key: str) -> Event:
    return _event(
        {
            "trace_domain": "document",
            "op": "kill",
            "route": route,
            "phase": "command",
            "tx_id": tx_id,
            "key": key,
        }
    )


def _commit(tx_id: int) -> Event:
    return _event(
        {"trace_domain": "tx", "op": "exit", "tx_id": tx_id, "tx_depth": 1, "outcome": "commit"}
    )


def _order_row(eid: str, *, qty: int = 0, total: str = "0") -> dict[str, Any]:
    return {"id": eid, "qty": qty, "total": Decimal(total)}


def _line_row(eid: str, order_id: str, *, qty: int = 0, amount: str = "0") -> dict[str, Any]:
    return {"id": eid, "order_id": order_id, "qty": qty, "amount": Decimal(amount), "kind": "sale"}


def _per_commit(law: Any, *events: Event) -> list[Any]:
    oracle = compile_oracle(law, per_commit=True)
    return check(History(seed=0, events=events), list(oracle.invariants))


class TestPerCommitPairedLaw:
    def test_an_over_allocation_is_caught_at_its_commit(self) -> None:
        violations = _per_commit(
            LINES_WITHIN_ORDER_QTY,
            _write(1, ORDERS.name, _order_row("o1", qty=3)),
            _write(1, LINES.name, _line_row("l1", "o1", qty=2)),
            _commit(1),
            _write(2, LINES.name, _line_row("l2", "o1", qty=2)),
            _commit(2),
        )

        assert len(violations) == 1
        assert "tx2" in violations[0].message
        assert "aggregate observed 4.0 against 3.0" in violations[0].message

    def test_raising_the_bound_first_keeps_the_law(self) -> None:
        violations = _per_commit(
            LINES_WITHIN_ORDER_QTY,
            _write(1, ORDERS.name, _order_row("o1", qty=3)),
            _write(1, LINES.name, _line_row("l1", "o1", qty=2)),
            _commit(1),
            _write(2, ORDERS.name, _order_row("o1", qty=4)),
            _commit(2),
            _write(3, LINES.name, _line_row("l2", "o1", qty=2)),
            _commit(3),
        )

        assert violations == []


class TestPerCommitExactDecimals:
    def test_tenths_sum_exactly_from_the_native_row(self) -> None:
        violations = _per_commit(
            LINES_WITHIN_ORDER_TOTAL,
            _write(1, ORDERS.name, _order_row("o1", total="0.3")),
            _write(1, LINES.name, _line_row("l1", "o1", amount="0.1")),
            _write(1, LINES.name, _line_row("l2", "o1", amount="0.2")),
            _commit(1),
        )

        assert violations == []

    def test_a_hundredth_over_is_caught(self) -> None:
        violations = _per_commit(
            LINES_WITHIN_ORDER_TOTAL,
            _write(1, ORDERS.name, _order_row("o1", total="0.3")),
            _write(1, LINES.name, _line_row("l1", "o1", amount="0.1")),
            _write(1, LINES.name, _line_row("l2", "o1", amount="0.21")),
            _commit(1),
        )

        assert len(violations) == 1
        assert "aggregate observed 0.31 against 0.3" in violations[0].message

    def test_a_decimal_seen_only_as_its_json_string_fails_closed(self) -> None:
        with pytest.raises(exc, match="must be numeric"):
            _per_commit(
                LINES_WITHIN_ORDER_TOTAL,
                _write(1, LINES.name, _line_row("l1", "o1", amount="0.1"), native=False),
                _commit(1),
            )


class TestPerCommitScopeWithNoRows:
    def test_removing_the_last_line_is_caught_and_removing_the_order_too_is_not(self) -> None:
        violations = _per_commit(
            ORDER_HAS_A_LINE,
            _write(1, ORDERS.name, _order_row("o1")),
            _write(1, LINES.name, _line_row("l1", "o1")),
            _commit(1),
            _write(2, ORDERS.name, _order_row("o2")),
            _write(2, LINES.name, _line_row("l2", "o2")),
            _commit(2),
            _kill(3, LINES.name, "l1"),  # o1 left with no lines
            _commit(3),
            _kill(4, LINES.name, "l2"),  # o2's last line and o2 itself, atomically
            _kill(4, ORDERS.name, "o2"),
            _commit(4),
        )

        assert len(violations) == 1
        assert "tx3" in violations[0].message
        assert "'order_id': 'o1'" in violations[0].message


# ----------------------- #
# End to end over a real run: the value-trace carries native decimals, both oracles agree.

_CAPTURE = SimulationConfig(seeds=range(1), cluster=ClusterConfig(nodes=1), capture_values=True)


def _cluster(node: Any, observe: Any) -> Cluster:
    return Cluster(
        deps=lambda state: MockDepsModule(state=state),
        state_factory=MockState,
        node=node,
        observe=observe,
    )


def _node(*, overrun: bool) -> Any:
    async def node(_node_id: int, ctx: ExecutionContext) -> None:
        async with ctx.tx_ctx.scope("mock"):
            order = await _order(ctx, total="0.3")
            await _line(ctx, order, amount="0.1")
            await _line(ctx, order, amount="0.2")

        if overrun:
            async with ctx.tx_ctx.scope("mock"):
                await _line(ctx, order, amount="0.01")

    return node


class TestEndToEnd:
    @pytest.mark.parametrize("overrun", [False, True])
    def test_both_oracles_agree_on_exact_decimals(self, overrun: bool) -> None:
        final = compile_oracle(LINES_WITHIN_ORDER_TOTAL)
        per_commit = compile_oracle(LINES_WITHIN_ORDER_TOTAL, per_commit=True)

        histories = _cluster(_node(overrun=overrun), final.observe).histories(_CAPTURE)

        for oracle in (final, per_commit):
            violations = [v for h in histories for v in check(h, list(oracle.invariants))]
            assert bool(violations) is overrun
