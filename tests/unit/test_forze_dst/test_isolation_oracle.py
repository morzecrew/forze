"""Transactional-isolation oracle — snapshot isolation + serializability.

Three layers:

1. the sound **kernel** (`find_*_violations`) over explicit :class:`TxRecord`s — verdicts + spectrum;
2. the **derivation** (`transactions_from_history`) grouping trace events by the per-event ``tx_id``
   seam — including the interleaved case that operation-span attribution gets wrong;
3. **end-to-end** — a real DST run proves ``tx_id`` flows from the transaction context through the
   port proxy and projection into the history, and that a write skew is caught from it.
"""

from __future__ import annotations

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig
from forze_dst.invariants import (
    TxRecord,
    VersionedTxRecord,
    find_serializability_cycle,
    find_serializable_violations,
    find_snapshot_isolation_violations,
    serializable,
    snapshot_isolation,
    transactions_from_history,
    versioned_transactions_from_history,
)
from forze_dst.oracle.recorder import Event, History
from forze_mock import MockDepsModule
from uuid import UUID

# ----------------------- #
# Layer 1 — the kernel over explicit read/write sets.


def _tx(
    name: str,
    *,
    start: int,
    end: int,
    reads: set[str] | None = None,
    writes: set[str] | None = None,
    committed: bool = True,
) -> TxRecord:
    return TxRecord(
        name=name,
        start=start,
        end=end,
        committed=committed,
        reads=frozenset(reads or set()),
        writes=frozenset(writes or set()),
    )


def _pair(a_reads=None, a_writes=None, b_reads=None, b_writes=None, **kw):  # type: ignore[no-untyped-def]
    # Two concurrent transactions: A spans [0, 10], B spans [4, 9] (overlapping).
    return [
        _tx(
            "A",
            start=0,
            end=10,
            reads=a_reads,
            writes=a_writes,
            committed=kw.get("a_ok", True),
        ),
        _tx(
            "B",
            start=4,
            end=9,
            reads=b_reads,
            writes=b_writes,
            committed=kw.get("b_ok", True),
        ),
    ]


class TestKernel:
    def test_lost_update_flagged_by_both(self) -> None:
        txns = _pair(a_reads={"x"}, a_writes={"x"}, b_reads={"x"}, b_writes={"x"})
        assert len(find_snapshot_isolation_violations(txns)) == 1
        assert len(find_serializable_violations(txns)) == 1

    def test_lost_update_not_flagged_when_one_aborted(self) -> None:
        txns = _pair(a_writes={"x"}, b_writes={"x"}, b_ok=False)
        assert find_snapshot_isolation_violations(txns) == []
        assert find_serializable_violations(txns) == []

    def test_write_skew_serializable_only(self) -> None:
        txns = _pair(
            a_reads={"x", "y"}, a_writes={"x"}, b_reads={"x", "y"}, b_writes={"y"}
        )
        assert find_snapshot_isolation_violations(txns) == []  # SI permits write skew
        violations = find_serializable_violations(txns)
        assert len(violations) == 1 and "write-skew" in violations[0].message

    def test_disjoint_keys_clean(self) -> None:
        txns = _pair(a_reads={"x"}, a_writes={"x"}, b_reads={"y"}, b_writes={"y"})
        assert find_serializable_violations(txns) == []
        assert find_snapshot_isolation_violations(txns) == []

    def test_sequential_never_conflict(self) -> None:
        txns = [
            _tx("A", start=0, end=3, writes={"x"}),
            _tx("B", start=4, end=9, writes={"x"}),
        ]
        assert find_serializable_violations(txns) == []

    def test_one_sided_anti_dependency_is_not_write_skew(self) -> None:
        txns = _pair(a_reads={"y"}, a_writes={"x"}, b_reads=set(), b_writes={"y"})
        assert find_serializable_violations(txns) == []

    def test_spectrum_superset(self) -> None:
        for txns in (
            _pair(a_writes={"x"}, b_writes={"x"}),
            _pair(
                a_reads={"x", "y"}, a_writes={"x"}, b_reads={"x", "y"}, b_writes={"y"}
            ),
        ):
            assert len(find_serializable_violations(txns)) >= len(
                find_snapshot_isolation_violations(txns)
            )


# ....................... #
# Layer 2 — derivation from a history, grouped by the per-event tx_id seam.


def _ev(seq: int, **fields: object) -> Event:
    return Event(
        seq=seq, kind="trace", at=float(seq), fields={"trace_seq": seq, **fields}
    )


def _read(seq: int, tx_id: int, key: str, route: str = "mock") -> Event:
    return _ev(
        seq,
        trace_domain="document",
        op="get",
        phase="query",
        key=key,
        route=route,
        tx_id=tx_id,
    )


def _wrt(seq: int, tx_id: int, key: str, route: str = "mock") -> Event:
    return _ev(
        seq,
        trace_domain="document",
        op="update",
        phase="command",
        key=key,
        route=route,
        tx_id=tx_id,
    )


def _enter(seq: int, tx_id: int) -> Event:
    return _ev(seq, trace_domain="tx", op="enter", tx_id=tx_id)


def _exit(seq: int, tx_id: int, *, outcome: str = "commit") -> Event:
    return _ev(seq, trace_domain="tx", op="exit", tx_id=tx_id, outcome=outcome)


class TestDerivation:
    def test_groups_interleaved_calls_by_tx_id(self) -> None:
        # The case operation-span attribution gets WRONG: tx 1's write (seq 5) lands AFTER tx 2
        # entered (seq 2), so it falls inside tx 2's span — yet tx_id attributes it correctly.
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _read(1, 1, "x"),
                _enter(2, 2),
                _read(3, 2, "x"),
                _read(4, 2, "y"),
                _wrt(5, 1, "x"),  # tx1 writes after tx2 started — interleaved
                _read(6, 1, "y"),
                _wrt(7, 2, "y"),
                _exit(8, 1),
                _exit(9, 2),
            ),
        )
        by = {tx.name: tx for tx in transactions_from_history(history)}

        assert by["tx1"].reads == frozenset({("mock", "x"), ("mock", "y")})
        assert by["tx1"].writes == frozenset(
            {("mock", "x")}
        )  # NOT misattributed to tx2
        assert by["tx2"].reads == frozenset({("mock", "x"), ("mock", "y")})
        assert by["tx2"].writes == frozenset({("mock", "y")})
        assert by["tx1"].committed and by["tx2"].committed
        # The two interleaved (concurrent) committed txns are a write skew.
        assert len(serializable()(history)) == 1
        assert snapshot_isolation()(history) == []

    def test_rolled_back_transaction_is_not_committed(self) -> None:
        # tx 2 has no exit event (it rolled back) → not committed → excluded.
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _wrt(1, 1, "x"),
                _exit(2, 1),
                _enter(3, 2),
                _wrt(4, 2, "x"),
            ),
        )
        by = {tx.name: tx for tx in transactions_from_history(history)}
        assert by["tx1"].committed and not by["tx2"].committed
        assert serializable()(history) == []  # only one committed → no conflict

    def test_a_rollback_outcome_exit_is_not_committed(self) -> None:
        # The exit fires from a finally on rollback too, so tx 2 DOES emit an exit — but with a
        # rollback outcome, so it must not be counted as committed (no false write-write conflict).
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _wrt(1, 1, "x"),
                _exit(2, 1),  # committed
                _enter(3, 2),
                _wrt(4, 2, "x"),
                _exit(5, 2, outcome="rollback"),  # rolled back, yet emits an exit
            ),
        )
        by = {tx.name: tx for tx in transactions_from_history(history)}
        assert by["tx1"].committed and not by["tx2"].committed
        assert (
            serializable()(history) == []
        )  # the rolled-back writer is not a conflicting committer

    def test_events_without_tx_id_are_ignored(self) -> None:
        history = History(
            seed=0, events=(_ev(0, phase="command", key="x", op="update"),)
        )
        assert transactions_from_history(history) == []


# ....................... #
# The COMPLETE kernel — a dependency cycle over version-aware (rev) read/write sets.


def _vtx(
    name: str,
    *,
    reads: tuple[tuple[str, int], ...] = (),
    writes: tuple[tuple[str, int], ...] = (),
    committed: bool = True,
) -> VersionedTxRecord:
    return VersionedTxRecord(
        name=name,
        start=0,
        end=10,
        committed=committed,
        commit_seq=10 if committed else None,
        reads=frozenset(reads),
        writes=frozenset(writes),
    )


class TestSerializabilityGraph:
    def test_write_skew_is_a_two_cycle(self) -> None:
        # A read y@1 → wrote x@2; B read x@1 → wrote y@2: mutual anti-dependency, a 2-cycle.
        a = _vtx("A", reads=(("y", 1),), writes=(("x", 2),))
        b = _vtx("B", reads=(("x", 1),), writes=(("y", 2),))
        violations = find_serializability_cycle([a, b])
        assert len(violations) == 1
        assert "dependency cycle" in violations[0].message

    def test_read_only_anomaly_is_a_three_cycle(self) -> None:
        # The case the pairwise checker structurally cannot see: a 3-cycle with NO mutual pair.
        t3 = _vtx("T3", reads=(("x", 1), ("y", 1)), writes=(("x", 2),))
        t2 = _vtx("T2", reads=(("y", 1),), writes=(("y", 2),))
        t1 = _vtx("T1", reads=(("x", 1), ("y", 2)))  # read-only
        violations = find_serializability_cycle([t1, t2, t3])
        assert len(violations) == 1
        message = violations[0].message
        assert "T1" in message and "T2" in message and "T3" in message

    def test_read_modify_write_lost_update_is_a_cycle(self) -> None:
        # Both read x@1; A wrote x@2, B wrote x@3 → ww A→B and rw B→A (B read what A overwrote).
        a = _vtx("A", reads=(("x", 1),), writes=(("x", 2),))
        b = _vtx("B", reads=(("x", 1),), writes=(("x", 3),))
        assert len(find_serializability_cycle([a, b])) == 1

    def test_serializable_history_has_no_cycle(self) -> None:
        # A then B: B read A's committed x@2 (wr A→B) — a chain, not a cycle.
        a = _vtx("A", reads=(("x", 1),), writes=(("x", 2),))
        b = _vtx("B", reads=(("x", 2),), writes=(("y", 1),))
        assert find_serializability_cycle([a, b]) == []

    def test_disjoint_keys_are_clean(self) -> None:
        a = _vtx("A", reads=(("x", 1),), writes=(("x", 2),))
        b = _vtx("B", reads=(("y", 1),), writes=(("y", 2),))
        assert find_serializability_cycle([a, b]) == []

    def test_aborted_transaction_is_excluded(self) -> None:
        # B would close the write-skew cycle, but it aborted — excluded, so the graph is acyclic.
        a = _vtx("A", reads=(("y", 1),), writes=(("x", 2),))
        b = _vtx("B", reads=(("x", 1),), writes=(("y", 2),), committed=False)
        assert find_serializability_cycle([a, b]) == []

    def test_long_dependency_chain_does_not_exhaust_recursion(self) -> None:
        # A 2000-deep serializable chain (each reads the prior version, writes the next): the
        # iterative cycle detector handles it without RecursionError and reports no cycle (acyclic).
        txns = [
            _vtx(f"T{i:05d}", reads=(("x", i),), writes=(("x", i + 1),))
            for i in range(2000)
        ]
        assert find_serializability_cycle(txns) == []


# ....................... #
# The versioned derivation — version-aware read/write sets from capture-mode result events.


def _read_v(seq: int, tx_id: int, key: str, rev: int, route: str = "mock") -> Event:
    return _ev(
        seq,
        trace_domain="document",
        op="get",
        phase="query",
        key=key,
        route=route,
        tx_id=tx_id,
        result={"id": key, "rev": rev},
    )


def _write_v(seq: int, tx_id: int, key: str, rev: int, route: str = "mock") -> Event:
    return _ev(
        seq,
        trace_domain="document",
        op="update",
        phase="command",
        key=key,
        route=route,
        tx_id=tx_id,
        result={"id": key, "rev": rev},
    )


def _create_v(
    seq: int, tx_id: int, new_id: str, rev: int, route: str = "mock"
) -> Event:
    # A create has NO leading-id `key` on its call, but its result event carries the minted id+rev.
    return _ev(
        seq,
        trace_domain="document",
        op="create",
        phase="command",
        route=route,
        tx_id=tx_id,
        result={"id": new_id, "rev": rev},
    )


class TestVersionedDerivation:
    def test_derives_versioned_reads_and_writes_with_rev(self) -> None:
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _read_v(1, 1, "x", 1),
                _write_v(2, 1, "x", 2),
                _exit(3, 1),
            ),
        )
        [tx] = versioned_transactions_from_history(history)
        assert tx.committed and tx.commit_seq == 3
        assert (("mock", "x"), 1) in tx.reads  # key is (namespace, id)
        assert (("mock", "x"), 2) in tx.writes

    def test_create_without_a_leading_key_is_recorded_from_its_result(self) -> None:
        # The key-only derivation misses a create (no leading `key`); the versioned one records it.
        history = History(
            seed=0, events=(_enter(0, 1), _create_v(1, 1, "new", 1), _exit(2, 1))
        )
        [tx] = versioned_transactions_from_history(history)
        assert (("mock", "new"), 1) in tx.writes

    def test_rolled_back_transaction_is_not_committed(self) -> None:
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _write_v(1, 1, "x", 2),
                _exit(2, 1, outcome="rollback"),
            ),
        )
        [tx] = versioned_transactions_from_history(history)
        assert not tx.committed

    def test_capture_off_fails_closed(self) -> None:
        # Keyed ops but no captured result events → the graph would be vacuously empty → raise.
        history = History(seed=0, events=(_enter(0, 1), _wrt(1, 1, "x"), _exit(2, 1)))
        with pytest.raises(exc, match="capture_values"):
            versioned_transactions_from_history(history)

    def test_no_keyed_ops_is_vacuous_not_an_error(self) -> None:
        history = History(seed=0, events=(_enter(0, 1), _exit(1, 1)))
        [tx] = versioned_transactions_from_history(history)
        assert tx.committed and tx.reads == frozenset() and tx.writes == frozenset()

    def test_complete_invariant_catches_a_read_only_anomaly_pairwise_misses(
        self,
    ) -> None:
        # The headline: the same history the pairwise check passes is caught by the complete one.
        history = History(
            seed=0,
            events=(
                _enter(0, 3),
                _read_v(1, 3, "x", 1),
                _read_v(2, 3, "y", 1),
                _enter(3, 2),
                _read_v(4, 2, "y", 1),
                _write_v(5, 2, "y", 2),
                _exit(6, 2),
                _enter(7, 1),
                _read_v(8, 1, "x", 1),
                _read_v(9, 1, "y", 2),
                _exit(10, 1),
                _write_v(11, 3, "x", 2),
                _exit(12, 3),
            ),
        )
        assert (
            serializable()(history) == []
        )  # pairwise: no two-transaction anomaly → missed
        assert len(serializable(complete=True)(history)) == 1  # complete: the 3-cycle

    def test_distinct_specs_sharing_an_id_do_not_conflate(self) -> None:
        # Two documents in DIFFERENT specs (namespaces) sharing the id "9": Ta reads users/5 and
        # blind-writes orders/9; Tb reads users/9 and writes users/5. The only real conflict is
        # users/5 (one-sided rw Ta→Tb) → serializable [Ta, Tb]. Keying on (namespace, id) must NOT
        # conflate orders/9 with users/9 into a spurious cross-spec 2-cycle.
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _read_v(1, 1, "5", 1, route="users"),
                _write_v(2, 1, "9", 2, route="orders"),
                _exit(3, 1),
                _enter(4, 2),
                _read_v(5, 2, "9", 1, route="users"),
                _write_v(6, 2, "5", 2, route="users"),
                _exit(7, 2),
            ),
        )
        assert serializable(complete=True)(history) == []  # no false-positive cycle


# ....................... #
# Layer 3 — end-to-end: tx_id flows from a real DST run into the history.


class Row(Document):
    value: int = 1


class RowCreate(CreateDocumentCmd):
    value: int = 1


class RowRead(ReadDocument):
    value: int


class RowUpdate(BaseDTO):
    value: int | None = None


ROW = DocumentSpec(
    name="iso_rows",
    read=RowRead,
    write=DocumentWriteTypes(domain=Row, create_cmd=RowCreate, update_cmd=RowUpdate),
)


class RowArg(BaseModel):
    row_id: UUID


@attrs.define(slots=True, kw_only=True)
class _Open(Handler[None, UUID]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> UUID:
        row = await self.ctx.document.command(ROW).create(RowCreate(value=1))
        return row.id


@attrs.define(slots=True, kw_only=True)
class _Touch(Handler[RowArg, None]):
    """Read then write one row inside a transaction — a keyed read + write under one tx_id."""

    ctx: ExecutionContext

    async def __call__(self, args: RowArg) -> None:
        row = await self.ctx.document.query(ROW).get(args.row_id)
        await self.ctx.document.command(ROW).update(
            args.row_id, row.rev, RowUpdate(value=row.value + 1)
        )


_TOUCH_PLAN = OperationPlan().bind_tx().set_route("mock").finish(deep=False)


def _registry() -> OperationRegistry:
    return OperationRegistry(
        handlers={
            "open": lambda ctx: _Open(ctx=ctx),
            "touch": lambda ctx: _Touch(ctx=ctx),
        },
        plans={"touch": _TOUCH_PLAN},
        descriptors={
            "open": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
            "touch": OperationDescriptor(
                input_type=RowArg, output_type=None, description="x"
            ),
        },
    ).freeze()


_SCENARIO = Scenario(
    state=ModelState,
    arrange=(Rule(op="open", produces="row"),),
    act=(
        Rule(
            op="touch",
            requires=("row",),
            arg=lambda state, rng: RowArg(row_id=state.pick("row", rng)),
        ),
    ),
)


class TestEndToEnd:
    def test_tx_id_flows_from_a_real_run_into_the_history(self) -> None:
        captured: list[History] = []

        def _capture(history: History) -> list:  # type: ignore[type-arg]
            captured.append(history)
            return []

        Simulation(
            operations=_registry(), deps=lambda: MockDepsModule(), invariants=[_capture]
        ).run(
            SimulationConfig(seeds=range(1), act_count=1, concurrency=1),
            scenario=_SCENARIO,
        )

        assert captured, "the simulation produced no history"
        history = captured[0]

        # The keyed port calls of the tx-scoped 'touch' op carry a real tx_id (the seam works).
        keyed = [
            e
            for e in history.of_kind("trace")
            if e.fields.get("key") is not None
            and e.fields.get("phase") in ("query", "command")
        ]
        assert keyed, "no keyed port calls were traced"
        assert all(e.fields.get("tx_id") is not None for e in keyed)

        # And the derivation reconstructs the transaction from that real trace.
        committed = [tx for tx in transactions_from_history(history) if tx.committed]
        assert len(committed) == 1
        assert committed[0].reads and committed[0].writes  # it read and wrote the row

    def test_complete_mode_has_no_false_positive_on_a_benign_run(self) -> None:
        # The complete oracle, wired into a real concurrent Simulation with capture on: the touch
        # workload (read+rev-guarded update of one row) is serializable — OCC aborts conflicting
        # writers, so the committed transactions form a chain, not a cycle. No false positive.
        report = Simulation(
            operations=_registry(),
            deps=lambda: MockDepsModule(),
            invariants=[serializable(complete=True)],
        ).run(
            SimulationConfig(
                seeds=range(8), act_count=4, concurrency=4, capture_values=True
            ),
            scenario=_SCENARIO,
        )

        assert report is None
