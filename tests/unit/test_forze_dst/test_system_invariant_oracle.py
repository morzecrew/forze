"""compile_oracle (RFC 0012 P3) — a SystemInvariant compiled into a DST oracle, checked over final state.

The oracle's observe hook issues ONE grouped aggregate per law and records the outcome for *every*
scope the run produced; the compiled invariant flags only the scopes whose aggregate failed the
predicate. These tests pin that: a conservation law (sum per group == 0) and a cardinality law
(count per group <= 1) each hold when every group is clean, flag exactly the offending group when one
is not, and respect the read-set's ``where`` filter — driven directly against the mock with a bound
recorder (no full simulation needed to exercise the oracle).
"""

from __future__ import annotations

import asyncio

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.invariants import (
    Count,
    ReadSet,
    Sum,
    SystemInvariant,
)
from forze.application.execution import ExecutionContext
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst.invariants import check, compile_oracle
from forze_dst.oracle import run_recorded
from forze_dst.oracle.recorder import Recorder, bind_recorder
from forze_dst.oracle.system_invariants import SYSTEM_INVARIANT_KIND
from forze_mock import MockDepsModule

from tests.support.execution_context import context_from_deps

# ----------------------- #


class Entry(Document):
    group: str
    amount: int = 0
    status: str = "active"


class EntryCreate(CreateDocumentCmd):
    group: str
    amount: int = 0
    status: str = "active"


class EntryRead(ReadDocument):
    group: str
    amount: int = 0
    status: str = "active"


ENTRIES = DocumentSpec(
    name="oracle_entries",
    read=EntryRead,
    write={"domain": Entry, "create_cmd": EntryCreate, "update_cmd": EntryCreate},
)

CONSERVATION = SystemInvariant(
    name="conservation",
    read_set=ReadSet(spec=ENTRIES, scope_keys=("group",)),
    aggregate=Sum("amount"),
    holds=lambda total: total == 0,
)

CARDINALITY = SystemInvariant(
    name="cardinality",
    read_set=ReadSet(
        spec=ENTRIES, scope_keys=("group",), where={"$values": {"status": "active"}}
    ),
    aggregate=Count(),
    holds=lambda n: n <= 1,
)


# ....................... #


def _ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


async def _add(ctx: ExecutionContext, group: str, *, amount: int = 0, status: str = "active") -> None:
    await ctx.document.command(ENTRIES).create(
        EntryCreate(group=group, amount=amount, status=status)
    )


async def _run(oracle, ctx: ExecutionContext):  # type: ignore[no-untyped-def]
    recorder = Recorder(seed=0)
    with bind_recorder(recorder):
        await oracle.observe(ctx)
    history = recorder.history
    return history, check(history, list(oracle.invariants))


# ----------------------- #


class TestConservationOracle:
    async def test_holds_when_every_group_sums_to_zero(self) -> None:
        ctx = _ctx()
        await _add(ctx, "L1", amount=100)
        await _add(ctx, "L1", amount=-100)
        await _add(ctx, "L2", amount=5)
        await _add(ctx, "L2", amount=-5)

        history, violations = await _run(compile_oracle(CONSERVATION), ctx)

        assert violations == []
        # Every scope the run produced was checked (not a hand-listed few).
        checked = {e.fields["scope"]["group"] for e in history.of_kind(SYSTEM_INVARIANT_KIND)}
        assert checked == {"L1", "L2"}

    async def test_flags_exactly_the_unbalanced_group(self) -> None:
        ctx = _ctx()
        await _add(ctx, "L1", amount=100)
        await _add(ctx, "L1", amount=-100)  # balanced
        await _add(ctx, "L2", amount=30)  # L2 unbalanced (+30)

        _, violations = await _run(compile_oracle(CONSERVATION), ctx)

        assert len(violations) == 1
        assert violations[0].invariant == "conservation"
        assert "L2" in violations[0].message
        assert "30" in violations[0].message


class TestCardinalityOracle:
    async def test_flags_the_double_charged_group(self) -> None:
        ctx = _ctx()
        await _add(ctx, "O1")  # one active entry
        await _add(ctx, "O2")
        await _add(ctx, "O2")  # O2 has two — the cardinality breach

        _, violations = await _run(compile_oracle(CARDINALITY), ctx)

        assert len(violations) == 1
        assert violations[0].invariant == "cardinality"
        assert "O2" in violations[0].message

    async def test_where_filter_excludes_out_of_scope_records(self) -> None:
        ctx = _ctx()
        await _add(ctx, "O1", status="active")
        await _add(ctx, "O1", status="voided")  # not counted (where: status == active)

        _, violations = await _run(compile_oracle(CARDINALITY), ctx)

        assert violations == []


class TestCompiledOracle:
    async def test_multiple_laws_checked_in_one_pass(self) -> None:
        ctx = _ctx()
        await _add(ctx, "G1", amount=10)  # conservation breach (+10) AND cardinality ok (1)
        await _add(ctx, "G2")
        await _add(ctx, "G2")  # cardinality breach (2) AND conservation ok (0)

        _, violations = await _run(compile_oracle(CONSERVATION, CARDINALITY), ctx)

        kinds = {v.invariant for v in violations}
        assert kinds == {"conservation", "cardinality"}

    async def test_no_records_holds_vacuously(self) -> None:
        ctx = _ctx()

        _, violations = await _run(compile_oracle(CONSERVATION), ctx)

        assert violations == []


# ....................... #
# Conservation under the simulation loop — the oracle runs over final state after a recorded run,
# the Sum-law analogue of the dst_payments cardinality demonstration.

_CONSERVATION_ORACLE = compile_oracle(CONSERVATION)


async def _open_entry(ctx: ExecutionContext, group: str, amount: int) -> str:
    entry = await ctx.document.command(ENTRIES).create(
        EntryCreate(group=group, amount=amount)
    )
    return entry.id


async def _atomic_transfer(
    ctx: ExecutionContext, src: str, dst: str, amount: int
) -> None:
    # One transaction, debit + credit — balanced, so conservation is preserved under any interleaving.
    async with ctx.tx_ctx.scope("mock"):
        source = await ctx.document.query(ENTRIES).get(src)
        dest = await ctx.document.query(ENTRIES).get(dst)
        await ctx.document.command(ENTRIES).update(
            src, source.rev, EntryCreate(group=source.group, amount=source.amount - amount)
        )
        await ctx.document.command(ENTRIES).update(
            dst, dest.rev, EntryCreate(group=dest.group, amount=dest.amount + amount)
        )


def _conservation_scenario(*, broken: bool):  # type: ignore[no-untyped-def]
    async def scenario() -> None:
        ctx = context_from_deps(MockDepsModule()())
        # Two ledgers, each opened balanced; concurrent transfers run on disjoint accounts.
        a = await _open_entry(ctx, "L1", 100)
        b = await _open_entry(ctx, "L1", -100)
        c = await _open_entry(ctx, "L2", 40)
        d = await _open_entry(ctx, "L2", -40)

        await asyncio.gather(
            _atomic_transfer(ctx, a, b, 30),
            _atomic_transfer(ctx, c, d, 10),
        )

        if broken:
            # A single-sided credit (no balancing debit) leaves L1 summing to +25.
            async with ctx.tx_ctx.scope("mock"):
                current = await ctx.document.query(ENTRIES).get(a)
                await ctx.document.command(ENTRIES).update(
                    a, current.rev, EntryCreate(group="L1", amount=current.amount + 25)
                )

        await _CONSERVATION_ORACLE.observe(ctx)

    return scenario


class TestConservationUnderSimulation:
    def test_atomic_transfers_preserve_conservation(self) -> None:
        history = run_recorded(
            _conservation_scenario(broken=False), seed=0, schedule_seed=0
        )

        assert check(history, list(_CONSERVATION_ORACLE.invariants)) == []

    def test_a_single_sided_write_is_caught_under_simulation(self) -> None:
        history = run_recorded(
            _conservation_scenario(broken=True), seed=0, schedule_seed=0
        )

        violations = check(history, list(_CONSERVATION_ORACLE.invariants))
        assert len(violations) == 1
        assert violations[0].invariant == "conservation"
        assert "L1" in violations[0].message
