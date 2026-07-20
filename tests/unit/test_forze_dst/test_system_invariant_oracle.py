"""compile_oracle — a SystemInvariant compiled into a DST oracle, checked over final state.

The oracle's observe hook issues ONE grouped aggregate per law and records the outcome for *every*
scope the run produced; the compiled invariant flags only the scopes whose aggregate failed the
predicate. These tests pin that: a conservation law (sum per group == 0) and a cardinality law
(count per group <= 1) each hold when every group is clean, flag exactly the offending group when one
is not, and respect the read-set's ``where`` filter — driven directly against the mock with a bound
recorder (no full simulation needed to exercise the oracle).
"""

from __future__ import annotations

import asyncio

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.invariants import (
    CountAll,
    ReadSet,
    SumOf,
    SystemInvariant,
)
from forze.application.execution import ExecutionContext
from forze.base.exceptions import exc
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
    aggregate=SumOf("amount"),
    holds=lambda total: total == 0,
)

CARDINALITY = SystemInvariant(
    name="cardinality",
    read_set=ReadSet(
        spec=ENTRIES, scope_keys=("group",), where={"$values": {"status": "active"}}
    ),
    aggregate=CountAll(),
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


# ....................... #
# Review-driven coverage: multi-scope violations, global laws, the v0 final-state bound, and the
# regressions for the confirmed correctness fixes (scope-key named "value", duplicate law names).


class _Tagged(Document):
    value: str  # a scope field literally named "value" — the old computed-alias collision case
    n: int = 0


class _TaggedCreate(CreateDocumentCmd):
    value: str
    n: int = 0


class _TaggedRead(ReadDocument):
    value: str
    n: int = 0


TAGGED = DocumentSpec(
    name="oracle_tagged",
    read=_TaggedRead,
    write={"domain": _Tagged, "create_cmd": _TaggedCreate, "update_cmd": _TaggedCreate},
)

GLOBAL_CONSERVATION = SystemInvariant(
    name="global_conservation",
    read_set=ReadSet(spec=ENTRIES, scope_keys=()),  # no scope_keys → one whole-collection check
    aggregate=SumOf("amount"),
    holds=lambda total: total == 0,
)

VALUE_SCOPED = SystemInvariant(
    name="value_scoped",
    read_set=ReadSet(spec=TAGGED, scope_keys=("value",)),  # scope key collides with the old alias
    aggregate=CountAll(),
    holds=lambda n: n <= 1,
)


class TestReviewDrivenCoverage:
    async def test_a_single_law_flags_every_unbalanced_scope(self) -> None:
        ctx = _ctx()
        await _add(ctx, "L1", amount=50)  # unbalanced
        await _add(ctx, "L2", amount=-20)  # unbalanced
        await _add(ctx, "L3", amount=10)
        await _add(ctx, "L3", amount=-10)  # balanced

        _, violations = await _run(compile_oracle(CONSERVATION), ctx)

        assert {v.invariant for v in violations} == {"conservation"}
        flagged = {
            scope
            for v in violations
            for scope in ("L1", "L2", "L3")
            if scope in v.message
        }
        assert flagged == {"L1", "L2"}  # both bad scopes, not L3

    async def test_global_law_checks_the_whole_collection(self) -> None:
        ctx = _ctx()
        await _add(ctx, "L1", amount=100)
        await _add(ctx, "L2", amount=-100)  # cross-ledger sum is 0

        history, violations = await _run(compile_oracle(GLOBAL_CONSERVATION), ctx)

        assert violations == []
        events = history.of_kind(SYSTEM_INVARIANT_KIND)
        assert len(events) == 1  # one whole-collection check
        assert events[0].fields["scope"] == {}  # global → empty scope

    async def test_global_law_catches_a_whole_collection_breach(self) -> None:
        ctx = _ctx()
        await _add(ctx, "L1", amount=100)
        await _add(ctx, "L2", amount=-40)  # total +60

        _, violations = await _run(compile_oracle(GLOBAL_CONSERVATION), ctx)

        assert len(violations) == 1
        assert "60" in violations[0].message

    async def test_final_state_only_misses_a_healed_transient(self) -> None:
        # The honest v0 bound: a violation that exists mid-run but is gone by the end is NOT caught.
        ctx = _ctx()
        await _add(ctx, "L1", amount=50)  # transiently +50 — would fail if checked here
        await _add(ctx, "L1", amount=-50)  # healed to 0 before the oracle observes

        _, violations = await _run(compile_oracle(CONSERVATION), ctx)

        assert violations == []  # final state holds; the transient is missed (per-commit fold is v1)

    async def test_scope_key_named_value_is_not_corrupted_by_the_aggregate(self) -> None:
        # Regression: the computed alias must not collide with a scope key named "value".
        ctx = _ctx()
        await ctx.document.command(TAGGED).create(_TaggedCreate(value="dup"))
        await ctx.document.command(TAGGED).create(_TaggedCreate(value="dup"))  # count 2 → breach

        history, violations = await _run(compile_oracle(VALUE_SCOPED), ctx)

        event = next(
            e for e in history.of_kind(SYSTEM_INVARIANT_KIND) if e.fields["scope"]
        )
        assert event.fields["scope"] == {"value": "dup"}  # the real scope value, not the count
        assert event.fields["observed"] == 2.0
        assert len(violations) == 1

    def test_duplicate_law_names_are_rejected(self) -> None:
        clash = SystemInvariant(
            name="conservation",  # same name as CONSERVATION
            read_set=ReadSet(spec=ENTRIES, scope_keys=("group",)),
            aggregate=CountAll(),
            holds=lambda n: n >= 0,
        )

        with pytest.raises(exc, match="duplicate law name"):
            compile_oracle(CONSERVATION, clash)
