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
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig
from forze_dst.invariants import (
    TxRecord,
    find_serializable_violations,
    find_snapshot_isolation_violations,
    serializable,
    snapshot_isolation,
    transactions_from_history,
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
        _tx("A", start=0, end=10, reads=a_reads, writes=a_writes, committed=kw.get("a_ok", True)),
        _tx("B", start=4, end=9, reads=b_reads, writes=b_writes, committed=kw.get("b_ok", True)),
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
        txns = _pair(a_reads={"x", "y"}, a_writes={"x"}, b_reads={"x", "y"}, b_writes={"y"})
        assert find_snapshot_isolation_violations(txns) == []  # SI permits write skew
        violations = find_serializable_violations(txns)
        assert len(violations) == 1 and "write-skew" in violations[0].message

    def test_disjoint_keys_clean(self) -> None:
        txns = _pair(a_reads={"x"}, a_writes={"x"}, b_reads={"y"}, b_writes={"y"})
        assert find_serializable_violations(txns) == []
        assert find_snapshot_isolation_violations(txns) == []

    def test_sequential_never_conflict(self) -> None:
        txns = [_tx("A", start=0, end=3, writes={"x"}), _tx("B", start=4, end=9, writes={"x"})]
        assert find_serializable_violations(txns) == []

    def test_one_sided_anti_dependency_is_not_write_skew(self) -> None:
        txns = _pair(a_reads={"y"}, a_writes={"x"}, b_reads=set(), b_writes={"y"})
        assert find_serializable_violations(txns) == []

    def test_spectrum_superset(self) -> None:
        for txns in (
            _pair(a_writes={"x"}, b_writes={"x"}),
            _pair(a_reads={"x", "y"}, a_writes={"x"}, b_reads={"x", "y"}, b_writes={"y"}),
        ):
            assert len(find_serializable_violations(txns)) >= len(
                find_snapshot_isolation_violations(txns)
            )


# ....................... #
# Layer 2 — derivation from a history, grouped by the per-event tx_id seam.


def _ev(seq: int, **fields: object) -> Event:
    return Event(seq=seq, kind="trace", at=float(seq), fields={"trace_seq": seq, **fields})


def _read(seq: int, tx_id: int, key: str) -> Event:
    return _ev(seq, trace_domain="document", op="get", phase="query", key=key, tx_id=tx_id)


def _wrt(seq: int, tx_id: int, key: str) -> Event:
    return _ev(seq, trace_domain="document", op="update", phase="command", key=key, tx_id=tx_id)


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

        assert by["tx1"].reads == frozenset({"x", "y"})
        assert by["tx1"].writes == frozenset({"x"})  # NOT misattributed to tx2
        assert by["tx2"].reads == frozenset({"x", "y"})
        assert by["tx2"].writes == frozenset({"y"})
        assert by["tx1"].committed and by["tx2"].committed
        # The two interleaved (concurrent) committed txns are a write skew.
        assert len(serializable()(history)) == 1
        assert snapshot_isolation()(history) == []

    def test_rolled_back_transaction_is_not_committed(self) -> None:
        # tx 2 has no exit event (it rolled back) → not committed → excluded.
        history = History(
            seed=0,
            events=(_enter(0, 1), _wrt(1, 1, "x"), _exit(2, 1), _enter(3, 2), _wrt(4, 2, "x")),
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
        assert serializable()(history) == []  # the rolled-back writer is not a conflicting committer

    def test_events_without_tx_id_are_ignored(self) -> None:
        history = History(seed=0, events=(_ev(0, phase="command", key="x", op="update"),))
        assert transactions_from_history(history) == []


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
        handlers={"open": lambda ctx: _Open(ctx=ctx), "touch": lambda ctx: _Touch(ctx=ctx)},
        plans={"touch": _TOUCH_PLAN},
        descriptors={
            "open": OperationDescriptor(input_type=None, output_type=None, description="x"),
            "touch": OperationDescriptor(input_type=RowArg, output_type=None, description="x"),
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
            if e.fields.get("key") is not None and e.fields.get("phase") in ("query", "command")
        ]
        assert keyed, "no keyed port calls were traced"
        assert all(e.fields.get("tx_id") is not None for e in keyed)

        # And the derivation reconstructs the transaction from that real trace.
        committed = [tx for tx in transactions_from_history(history) if tx.committed]
        assert len(committed) == 1
        assert committed[0].reads and committed[0].writes  # it read and wrote the row
