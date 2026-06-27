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
from forze.application.execution.tracing.port_proxy import REDACTED
from forze.base.exceptions import exc
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_dst import ModelState, Rule, Scenario, Simulation, SimulationConfig
from forze_dst.invariants import (
    ScanRead,
    TxRecord,
    VersionedTxRecord,
    WriteVersion,
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
from ipaddress import IPv4Address
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
# The COMPLETE kernel — predicate (phantom) anti-dependency edges (Phase 2).


def _scanner(
    name: str,
    *,
    ns: str,
    predicate: dict[str, object] | None,
    scan_seq: int,
    commit_seq: int,
    reads: tuple[tuple[tuple[str, str], int], ...] = (),
) -> VersionedTxRecord:
    return VersionedTxRecord(
        name=name,
        start=0,
        end=commit_seq,
        committed=True,
        commit_seq=commit_seq,
        reads=frozenset(reads),
        writes=frozenset(),
        scans=(ScanRead(namespace=ns, predicate=predicate, seq=scan_seq),),
    )


def _writer(
    name: str,
    *,
    key: tuple[str, str],
    rev: int,
    row: dict[str, object],
    commit_seq: int,
) -> VersionedTxRecord:
    return VersionedTxRecord(
        name=name,
        start=0,
        end=commit_seq,
        committed=True,
        commit_seq=commit_seq,
        reads=frozenset(),
        writes=frozenset({(key, rev)}),
        write_rows=(WriteVersion(key=key, rev=rev, row=row),),
    )


class TestPhantomEdges:
    def test_appearance_phantom_is_a_two_cycle(self) -> None:
        # Scanner read predicate {value: 7} at seq 1 (saw nothing); writer inserted a matching row
        # committed at seq 5 (> 1) → rw scanner→writer; the scanner's re-scan saw it as a hit
        # (read x@1) → wr writer→scanner. Together a 2-cycle the keyed checker cannot see.
        x = ("cells", "x")
        scanner = _scanner(
            "t1",
            ns="cells",
            predicate={"$values": {"value": 7}},
            scan_seq=1,
            commit_seq=12,
            reads=((x, 1),),
        )
        writer = _writer(
            "t2", key=x, rev=1, row={"id": "x", "rev": 1, "value": 7}, commit_seq=5
        )
        violations = find_serializability_cycle([scanner, writer])
        assert len(violations) == 1 and "predicate" in violations[0].message

    def test_writer_committed_before_scan_is_not_a_phantom(self) -> None:
        # commit_seq (5) <= scan_seq (9): the scan could have seen the write → no forward edge.
        x = ("cells", "x")
        scanner = _scanner(
            "t1",
            ns="cells",
            predicate={"$values": {"value": 7}},
            scan_seq=9,
            commit_seq=12,
        )
        writer = _writer(
            "t2", key=x, rev=1, row={"id": "x", "rev": 1, "value": 7}, commit_seq=5
        )
        assert find_serializability_cycle([scanner, writer]) == []

    def test_non_matching_row_adds_no_edge(self) -> None:
        # Predicate {value: 99} vs produced row value 7 — predicate-precise: no spurious edge.
        x = ("cells", "x")
        scanner = _scanner(
            "t1",
            ns="cells",
            predicate={"$values": {"value": 99}},
            scan_seq=1,
            commit_seq=12,
            reads=((x, 1),),
        )
        writer = _writer(
            "t2", key=x, rev=1, row={"id": "x", "rev": 1, "value": 7}, commit_seq=5
        )
        assert find_serializability_cycle([scanner, writer]) == []

    def test_match_all_scan_matches_any_namespace_writer(self) -> None:
        # A None predicate is match-all → a concurrent insert in the scanned namespace is a phantom.
        x = ("cells", "x")
        scanner = _scanner(
            "t1", ns="cells", predicate=None, scan_seq=1, commit_seq=12, reads=((x, 1),)
        )
        writer = _writer("t2", key=x, rev=1, row={"id": "x", "rev": 1}, commit_seq=5)
        assert len(find_serializability_cycle([scanner, writer])) == 1

    def test_other_namespace_write_is_ignored(self) -> None:
        # The matching write lands in a DIFFERENT namespace than the scan → no predicate edge.
        other = ("orders", "x")
        scanner = _scanner(
            "t1",
            ns="cells",
            predicate=None,
            scan_seq=1,
            commit_seq=12,
            reads=((other, 1),),
        )
        writer = _writer("t2", key=other, rev=1, row={"id": "x", "rev": 1}, commit_seq=5)
        assert find_serializability_cycle([scanner, writer]) == []

    def test_predicate_over_redacted_field_is_skipped(self) -> None:
        # Both the filter and the row carry the mask, so a naive match would be spurious — the oracle
        # refuses to reason over a redacted predicate (a false-negative, never a false-positive).
        x = ("cells", "x")
        scanner = _scanner(
            "t1",
            ns="cells",
            predicate={"$values": {"ssn": REDACTED}},
            scan_seq=1,
            commit_seq=12,
            reads=((x, 1),),
        )
        writer = _writer(
            "t2", key=x, rev=1, row={"id": "x", "rev": 1, "ssn": REDACTED}, commit_seq=5
        )
        assert find_serializability_cycle([scanner, writer]) == []

    def test_scanner_self_insert_is_not_a_phantom(self) -> None:
        # A transaction that scans then inserts its own matching row sees its own write — no self
        # anti-dependency (the writer-is-scanner case is excluded).
        x = ("cells", "x")
        tx = VersionedTxRecord(
            name="t1",
            start=0,
            end=10,
            committed=True,
            commit_seq=10,
            reads=frozenset(),
            writes=frozenset({(x, 1)}),
            scans=(ScanRead(namespace="cells", predicate=None, seq=1),),
            write_rows=(WriteVersion(key=x, rev=1, row={"id": "x", "rev": 1}),),
        )
        assert find_serializability_cycle([tx]) == []

    def test_native_typed_row_does_not_falsely_match_a_string_filter(self) -> None:
        # The cardinal-sin regression: the backend scans the NATIVE row, so a string filter must be
        # matched against the native value (``IPv4Address('10.0.0.1') != '10.0.0.1'``), not a JSON
        # string copy. S scans ip==str and writes y@2; W reads y@1 (→ a real rw W→S) and inserts a
        # device whose ip is the NATIVE address. The live scan would not return it, so the history is
        # serializable — a JSON-string row would spuriously match and close a false S→W→S cycle.
        y, dev = ("mock", "y"), ("devices", "x")
        scanner = VersionedTxRecord(
            name="S",
            start=0,
            end=10,
            committed=True,
            commit_seq=10,
            reads=frozenset({(y, 1)}),
            writes=frozenset({(y, 2)}),
            scans=(
                ScanRead(
                    namespace="devices",
                    predicate={"$values": {"ip": {"$eq": "10.0.0.1"}}},
                    seq=1,
                ),
            ),
        )
        writer = VersionedTxRecord(
            name="W",
            start=0,
            end=5,
            committed=True,
            commit_seq=5,
            reads=frozenset({(y, 1)}),
            writes=frozenset({(dev, 1)}),
            write_rows=(
                WriteVersion(
                    key=dev,
                    rev=1,
                    row={"id": "x", "rev": 1, "ip": IPv4Address("10.0.0.1")},
                ),
            ),
        )
        assert find_serializability_cycle([scanner, writer]) == []  # native ip ≠ str → no FP

        # Contrast: a genuine string ip field DOES match → the real phantom cycle is still caught,
        # proving the test discriminates (the fix narrows to the representation gap, not all matches).
        writer_str = attrs.evolve(
            writer,
            write_rows=(
                WriteVersion(key=dev, rev=1, row={"id": "x", "rev": 1, "ip": "10.0.0.1"}),
            ),
        )
        assert len(find_serializability_cycle([scanner, writer_str])) == 1


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
    # A command result carries both the JSON `result` and the NATIVE `result_native` (here identical,
    # since id/rev are JSON-stable) — the oracle reads the native form for predicate matching.
    return _ev(
        seq,
        trace_domain="document",
        op="update",
        phase="command",
        key=key,
        route=route,
        tx_id=tx_id,
        result={"id": key, "rev": rev},
        result_native={"id": key, "rev": rev},
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
        result_native={"id": new_id, "rev": rev},
    )


def _scan_call(
    seq: int,
    tx_id: int,
    predicate: dict[str, object] | None,
    *,
    op: str = "count",
    route: str = "mock",
) -> Event:
    # A predicate-read CALL event: the captured filter rides on `payload`; no single-entity `result`.
    return _ev(
        seq,
        trace_domain="document",
        op=op,
        phase="query",
        route=route,
        tx_id=tx_id,
        payload=predicate,
    )


def _find_hit(
    seq: int, tx_id: int, key: str, rev: int, *, route: str = "mock", **row: object
) -> Event:
    # A find_many HIT: a query-phase result event carrying the row → folds into the keyed read set.
    return _ev(
        seq,
        trace_domain="document",
        op="find_many",
        phase="query",
        route=route,
        tx_id=tx_id,
        result={"id": key, "rev": rev, **row},
    )


def _create_row(
    seq: int,
    tx_id: int,
    new_id: str,
    rev: int,
    *,
    route: str = "mock",
    native: dict[str, object] | None = None,
    **row: object,
) -> Event:
    # A create whose result carries the full produced row (the fields a predicate is matched against).
    # ``result`` is the JSON form (timeline/bundle); ``result_native`` is the native-typed form the
    # oracle matches predicates against — identical here unless ``native`` overrides with object types.
    json_row = {"id": new_id, "rev": rev, **row}
    native_row = (
        {"id": new_id, "rev": rev, **native} if native is not None else dict(json_row)
    )
    return _ev(
        seq,
        trace_domain="document",
        op="create",
        phase="command",
        route=route,
        tx_id=tx_id,
        result=json_row,
        result_native=native_row,
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


class TestPredicatePhantomDerivation:
    def test_a_scan_call_becomes_a_predicate_read(self) -> None:
        # A scan op's call event (filter on payload, no single-entity result) is recorded as a scan.
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _scan_call(1, 1, {"$values": {"value": 7}}, op="count"),
                _exit(2, 1),
            ),
        )
        [tx] = versioned_transactions_from_history(history)
        assert len(tx.scans) == 1
        scan = tx.scans[0]
        assert scan.namespace == "mock" and scan.predicate == {"$values": {"value": 7}}
        assert scan.seq == 1

    def test_find_many_hit_folds_into_the_read_set(self) -> None:
        # A find_many hit (a query-phase result event) is a keyed read, plus a scan from its call.
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _scan_call(1, 1, {"$values": {"value": 7}}, op="find_many"),
                _find_hit(2, 1, "x", 3, value=7),
                _exit(3, 1),
            ),
        )
        [tx] = versioned_transactions_from_history(history)
        assert (("mock", "x"), 3) in tx.reads  # the hit is a versioned read
        assert len(tx.scans) == 1  # and the call is the scan predicate

    def test_predicate_phantom_cycle_from_capture(self) -> None:
        # The headline P2: tx1 scans value==7 (sees nothing), tx2 inserts a matching row + commits,
        # tx1 re-scans (find_many) and now sees it (a hit). The captured filter directs rw tx1→tx2,
        # the hit directs wr tx2→tx1 → a 2-cycle the pairwise (and keyed-only complete) check misses.
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _scan_call(1, 1, {"$values": {"value": 7}}, op="count"),
                _enter(2, 2),
                _create_row(3, 2, "x", 1, value=7),
                _exit(4, 2),  # tx2 commits at seq 4 (> tx1's scan seq 1)
                _scan_call(5, 1, {"$values": {"value": 7}}, op="find_many"),
                _find_hit(6, 1, "x", 1, value=7),  # the re-scan sees tx2's row
                _exit(7, 1),
            ),
        )
        assert serializable()(history) == []  # pairwise: no two-txn keyed anomaly → missed
        violations = serializable(complete=True)(history)
        assert len(violations) == 1 and "predicate" in violations[0].message

    def test_non_matching_concurrent_insert_is_serializable(self) -> None:
        # tx1 scans value==7; tx2 concurrently inserts value==1 (no match). No phantom → serializable.
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _scan_call(1, 1, {"$values": {"value": 7}}, op="find_many"),
                _enter(2, 2),
                _create_row(3, 2, "x", 1, value=1),
                _exit(4, 2),
                _exit(5, 1),
            ),
        )
        assert serializable(complete=True)(history) == []

    def test_count_only_phantom_is_a_documented_false_negative(self) -> None:
        # A pure count predicate read contributes the forward rw edge but, capturing no rows, cannot
        # supply the reverse wr edge → the 2-transaction count phantom is a documented false-negative
        # (sound: a missing edge never manufactures a cycle). find_many — which captures hits — does
        # close it (see test_predicate_phantom_cycle_from_capture).
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _scan_call(1, 1, {"$values": {"value": 7}}, op="count"),
                _enter(2, 2),
                _create_row(3, 2, "x", 1, value=7),
                _exit(4, 2),
                _scan_call(5, 1, {"$values": {"value": 7}}, op="count"),  # re-count, no hits
                _exit(6, 1),
            ),
        )
        assert serializable(complete=True)(history) == []

    def test_native_write_row_prevents_a_json_string_false_positive(self) -> None:
        # Full pipeline regression for the JSON-vs-native gap: the capture records the device ip as a
        # JSON string in ``result`` but the native IPv4Address in ``result_native``; the oracle matches
        # the string filter against the native form → no match → no phantom edge. The only real edge is
        # W→S on key y, so the history is serializable. (Were the oracle to match the JSON string row,
        # ip=="10.0.0.1" would spuriously match and close a false S→W→S cycle.)
        history = History(
            seed=0,
            events=(
                _enter(0, 1),  # S
                _scan_call(
                    1, 1, {"$values": {"ip": {"$eq": "10.0.0.1"}}}, op="find_many", route="devices"
                ),
                _read_v(2, 1, "y", 1),
                _enter(3, 2),  # W
                _read_v(4, 2, "y", 1),
                _create_row(
                    5, 2, "x", 1, route="devices", ip="10.0.0.1", native={"ip": IPv4Address("10.0.0.1")}
                ),
                _exit(6, 2),  # commit_seq 6 > scan seq 1 (so the predicate edge IS considered)
                _write_v(7, 1, "y", 2),
                _exit(8, 1),
            ),
        )
        # the captured write row is the NATIVE address (the form the backend scans), not the JSON str
        rows = [
            wv.row
            for tx in versioned_transactions_from_history(history)
            for wv in tx.write_rows
        ]
        assert any(isinstance(row.get("ip"), IPv4Address) for row in rows)
        assert serializable(complete=True)(history) == []  # no JSON-vs-native false positive

    def test_find_stream_is_not_recorded_as_a_predicate_read(self) -> None:
        # find_stream is a lazy generator (its rows are read during iteration, after the call seq), so
        # commit_seq > scan.seq is not a sound "did not see" bound for it — it is excluded from the
        # predicate path, contributing no ScanRead (a forward edge from it could be wrong-direction).
        history = History(
            seed=0,
            events=(
                _enter(0, 1),
                _scan_call(1, 1, {"$values": {"value": 7}}, op="find_stream"),
                _exit(2, 1),
            ),
        )
        [tx] = versioned_transactions_from_history(history)
        assert tx.scans == ()


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


@attrs.define(slots=True, kw_only=True)
class _Scan(Handler[None, None]):
    """Scan ``value == 1`` inside a transaction — matches the opened rows, so hits are captured."""

    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.document.query(ROW).find_many({"$values": {"value": 1}})


@attrs.define(slots=True, kw_only=True)
class _ScanMiss(Handler[None, None]):
    """Scan ``value == 0`` — which no opened row (``value == 1``) matches, so no phantom can form."""

    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        await self.ctx.document.query(ROW).find_many({"$values": {"value": 0}})


_SCAN_PLAN = OperationPlan().bind_tx().set_route("mock").finish(deep=False)


def _scan_registry() -> OperationRegistry:
    return OperationRegistry(
        handlers={
            "open": lambda ctx: _Open(ctx=ctx),
            "scan": lambda ctx: _Scan(ctx=ctx),
            "scan_miss": lambda ctx: _ScanMiss(ctx=ctx),
        },
        plans={"scan": _SCAN_PLAN, "scan_miss": _SCAN_PLAN},
        descriptors={
            "open": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
            "scan": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
            "scan_miss": OperationDescriptor(
                input_type=None, output_type=None, description="x"
            ),
        },
    ).freeze()


_SCAN_SCENARIO = Scenario(
    state=ModelState,
    arrange=(Rule(op="open", produces="row"),),
    act=(Rule(op="scan"),),
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

    def test_scan_filter_and_hits_are_captured_into_history(self) -> None:
        # The Phase-2 capture path end-to-end: a real run records a scan's filter (a ScanRead over the
        # spec namespace) and its returned rows (versioned reads), so the predicate oracle has both the
        # predicate and the rows it needs — the derivation tests then turn these into phantom edges.
        captured: list[History] = []

        def _grab(history: History) -> list:  # type: ignore[type-arg]
            captured.append(history)
            return []

        Simulation(
            operations=_scan_registry(),
            deps=lambda: MockDepsModule(),
            invariants=[_grab],
        ).run(
            SimulationConfig(
                seeds=range(1), act_count=1, concurrency=1, capture_values=True
            ),
            scenario=_SCAN_SCENARIO,
        )

        assert captured, "the simulation produced no history"
        txns = versioned_transactions_from_history(captured[0])

        scans = [scan for tx in txns for scan in tx.scans]
        assert any(
            scan.predicate == {"$values": {"value": 1}} and scan.namespace == "iso_rows"
            for scan in scans
        ), "the scan's filter was not captured as a predicate read"
        # the matching row the scan returned was captured as a versioned read (a find_many hit)
        assert any(tx.reads for tx in txns), "the scan's hit was not captured as a read"

    def test_complete_mode_has_no_false_positive_on_a_scan_workload(self) -> None:
        # The complete oracle on a real concurrent scan+create workload: ops interleave a scan
        # (``value == 0``) with opens (``value == 1``). The scan predicate matches none of the
        # concurrently-created rows, so no phantom is possible — yet the matcher still runs over every
        # captured row. A clean run proves the predicate path adds no false positive on real traces.
        report = Simulation(
            operations=_scan_registry(),
            deps=lambda: MockDepsModule(),
            invariants=[serializable(complete=True)],
        ).run(
            SimulationConfig(
                seeds=range(8), act_count=4, concurrency=4, capture_values=True
            ),
            scenario=Scenario(
                state=ModelState,
                arrange=(Rule(op="open", produces="row"),),
                act=(Rule(op="scan_miss"), Rule(op="open", produces="row")),
            ),
        )

        assert report is None

    def test_trace_seq_is_globally_monotonic_across_concurrent_txns(self) -> None:
        # The predicate-edge direction (commit_seq vs scan.seq) compares sequences ACROSS transactions,
        # which is sound only if trace_seq is one globally-monotonic counter over a single shared trace
        # buffer. Pin that runtime invariant: a real concurrent capture run's folded trace has strictly
        # increasing, unique trace_seq — so a per-task buffer regression would fail here, not silently.
        captured: list[History] = []

        def _grab(history: History) -> list:  # type: ignore[type-arg]
            captured.append(history)
            return []

        Simulation(
            operations=_scan_registry(),
            deps=lambda: MockDepsModule(),
            invariants=[_grab],
        ).run(
            SimulationConfig(
                seeds=range(1), act_count=4, concurrency=4, capture_values=True
            ),
            scenario=Scenario(
                state=ModelState,
                arrange=(Rule(op="open", produces="row"),),
                act=(Rule(op="scan"), Rule(op="open", produces="row")),
            ),
        )

        seqs = [int(e.fields["trace_seq"]) for e in captured[0].of_kind("trace")]
        assert seqs == sorted(seqs), "trace_seq is not globally monotonic"
        assert len(seqs) == len(set(seqs)), "trace_seq is not unique across transactions"
