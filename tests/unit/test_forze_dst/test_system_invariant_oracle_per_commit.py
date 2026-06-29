"""compile_oracle(per_commit=True) — the per-commit trace fold.

The v0 oracle checks final state; v1 reconstructs the read-set's aggregate AS-OF EACH committed
transaction from the value-trace and asserts the predicate after every commit — so a violation a
concurrent interleaving creates and a later transaction heals is caught at the commit where it
existed (v0 misses it). These tests synthesize a History of trace events directly, so the exact
commit sequence, the transient, uncommitted writes, and nested savepoints are all under control —
proving the fold logic precisely without a full simulation.
"""

from __future__ import annotations

from typing import Any

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
from forze_dst import Cluster, SimulationConfig
from forze_dst.cluster import ClusterConfig
from forze_dst.invariants import check, compile_oracle
from forze_dst.oracle.recorder import Event, History
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #


class _Entry(Document):
    group: str
    amount: int = 0
    status: str = "active"


class _EntryCreate(CreateDocumentCmd):
    group: str
    amount: int = 0
    status: str = "active"


class _EntryRead(ReadDocument):
    group: str
    amount: int = 0
    status: str = "active"


ENTRIES = DocumentSpec(
    name="pc_entries",
    read=_EntryRead,
    write={"domain": _Entry, "create_cmd": _EntryCreate, "update_cmd": _EntryCreate},
)
ROUTE = ENTRIES.name

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
# Synthesized trace events: a write is a captured result event (the full post-write entity); a commit
# is a root tx-exit; nested savepoints exit at depth > 1.

_seq = iter(range(1, 10_000))


def _entity(eid: str, group: str, amount: int = 0, status: str = "active") -> dict[str, Any]:
    return {"id": eid, "group": group, "amount": amount, "status": status, "rev": 1}


def _write(tx_id: int, entity: dict[str, Any]) -> Event:
    seq = next(_seq)
    return Event(
        seq=seq,
        kind="trace",
        at=float(seq),
        fields={
            "trace_domain": "document",
            "op": "update",
            "surface": "document_command",
            "route": ROUTE,
            "phase": "command",
            "tx_id": tx_id,
            "trace_seq": seq,
            "result": entity,
        },
    )


def _commit(tx_id: int, *, depth: int = 1, outcome: str = "commit") -> Event:
    seq = next(_seq)
    return Event(
        seq=seq,
        kind="trace",
        at=float(seq),
        fields={
            "trace_domain": "tx",
            "op": "exit",
            "tx_id": tx_id,
            "tx_depth": depth,
            "outcome": outcome,
            "trace_seq": seq,
        },
    )


def _rollback(tx_id: int) -> Event:
    """A root tx-exit recorded for a ROLLED-BACK scope (the exit fires from a finally either way)."""

    return _commit(tx_id, outcome="rollback")


def _delete(tx_id: int, eid: str) -> Event:
    """A delete: a command event with a key but NO captured entity result (kill returns no model)."""

    seq = next(_seq)
    return Event(
        seq=seq,
        kind="trace",
        at=float(seq),
        fields={
            "trace_domain": "document",
            "op": "kill",
            "surface": "document_command",
            "route": ROUTE,
            "phase": "command",
            "tx_id": tx_id,
            "trace_seq": seq,
            "key": eid,
        },
    )


def _kill_many(tx_id: int) -> Event:
    """A bulk delete: a command event with NO per-row key (kill_many takes a list of pks)."""

    seq = next(_seq)
    return Event(
        seq=seq,
        kind="trace",
        at=float(seq),
        fields={
            "trace_domain": "document",
            "op": "kill_many",
            "surface": "document_command",
            "route": ROUTE,
            "phase": "command",
            "tx_id": tx_id,
            "trace_seq": seq,
        },
    )


def _history(*events: Event) -> History:
    return History(seed=0, events=tuple(events))


def _violations(law: SystemInvariant, history: History) -> list:
    oracle = compile_oracle(law, per_commit=True)
    return check(history, list(oracle.invariants))


# ----------------------- #


class TestPerCommitConservation:
    def test_catches_a_transient_that_a_later_commit_heals(self) -> None:
        # tx1: open balanced (sum 0). tx2: debit A only (sum -30, COMMITTED — observable). tx3: credit
        # B (sum 0 again). Final state is balanced, so v0 holds — but v1 catches the tx2 transient.
        history = _history(
            _write(1, _entity("a", "L1", 100)),
            _write(1, _entity("b", "L1", -100)),
            _commit(1),
            _write(2, _entity("a", "L1", 70)),  # debit committed alone → sum -30
            _commit(2),
            _write(3, _entity("b", "L1", -70)),  # credit → sum 0, healed
            _commit(3),
        )

        violations = _violations(CONSERVATION, history)

        assert len(violations) == 1
        assert violations[0].invariant == "conservation"
        assert "tx2" in violations[0].message  # caught at the offending commit
        assert "L1" in violations[0].message

    def test_atomic_transfers_never_violate(self) -> None:
        # Every transfer debits and credits within ONE transaction, so every commit is balanced.
        history = _history(
            _write(1, _entity("a", "L1", 100)),
            _write(1, _entity("b", "L1", -100)),
            _commit(1),
            _write(2, _entity("a", "L1", 70)),  # debit + credit together in tx2
            _write(2, _entity("b", "L1", -70)),
            _commit(2),
        )

        assert _violations(CONSERVATION, history) == []

    def test_an_uncommitted_transaction_is_not_applied(self) -> None:
        # tx2 debits A but never commits (rolled back) → its write must be ignored; tx1 alone holds.
        history = _history(
            _write(1, _entity("a", "L1", 100)),
            _write(1, _entity("b", "L1", -100)),
            _commit(1),
            _write(2, _entity("a", "L1", 70)),  # would imbalance, but tx2 never exits
        )

        assert _violations(CONSERVATION, history) == []

    def test_a_rolled_back_transaction_is_not_applied(self) -> None:
        # The exit event fires from a finally on rollback too, so a rolled-back debit still produces
        # an exit — it must be excluded by its rollback outcome, or the fold would see a phantom -30.
        history = _history(
            _write(1, _entity("a", "L1", 100)),
            _write(1, _entity("b", "L1", -100)),
            _commit(1),
            _write(2, _entity("a", "L1", 70)),  # debit, but tx2 rolls back
            _rollback(2),
        )

        assert _violations(CONSERVATION, history) == []

    def test_commits_are_folded_in_commit_order_not_write_order(self) -> None:
        # tx_a writes A=+30 first in the trace, but tx_b commits first (lower exit seq). The fold must
        # apply in COMMIT order: after tx_b the sum is -30 (B alone), so the violation is reported at
        # tx_b with observed -30 — not at tx_a with +30 (which write-order would give).
        history = _history(
            _write(101, _entity("a", "L1", 30)),  # written first…
            _write(102, _entity("b", "L1", -30)),  # …then this
            _commit(102),  # but tx 102 commits first (lower exit seq)
            _commit(101),  # tx 101 commits second
        )

        violations = _violations(CONSERVATION, history)

        assert len(violations) == 1
        assert "tx102" in violations[0].message  # the commit-order first commit
        assert "-30" in violations[0].message  # B alone, not A's +30

    def test_only_the_first_failure_per_scope_is_reported(self) -> None:
        # A scope that fails, heals, then fails again is reported once — at its first failing commit.
        history = _history(
            _write(1, _entity("a", "L1", 100)),
            _write(1, _entity("b", "L1", -100)),
            _commit(1),  # sum 0
            _write(2, _entity("a", "L1", 130)),
            _commit(2),  # sum +30 — first failure
            _write(3, _entity("a", "L1", 100)),
            _commit(3),  # sum 0 — healed
            _write(4, _entity("a", "L1", 150)),
            _commit(4),  # sum +50 — fails again
        )

        violations = _violations(CONSERVATION, history)

        assert len(violations) == 1
        assert "tx2" in violations[0].message  # only the first failure, not tx4

    def test_deletes_are_folded(self) -> None:
        # A committed delete (``kill``) drops the row from the fold by its key. Deleting A leaves the
        # sum at -100, breaking conservation — and the per-commit oracle now catches it at the commit
        # where it happened, instead of letting A linger and miss (or, for a Count law, falsely flag).
        history = _history(
            _write(1, _entity("a", "L1", 100)),
            _write(1, _entity("b", "L1", -100)),
            _commit(1),  # sum 0 — holds
            _delete(2, "a"),  # remove A → sum -100
            _commit(2),
        )

        violations = _violations(CONSERVATION, history)
        assert len(violations) == 1
        assert "tx2" in violations[0].message

    def test_a_delete_does_not_cause_a_false_count_violation(self) -> None:
        # The issue: a Count<=1 law must not fire on a row deleted before the commit. tx2 adds p2 and
        # removes p1 in the same group, so the live count is 1 and the law holds — a lingering p1 would
        # wrongly make it 2 and flag a violation that never existed.
        law = SystemInvariant(
            name="one_per_group",
            read_set=ReadSet(spec=ENTRIES, scope_keys=("group",)),
            aggregate=CountAll(),
            holds=lambda n: n <= 1,
        )
        history = _history(
            _write(1, _entity("p1", "O1")),
            _commit(1),  # count 1 — holds
            _write(2, _entity("p2", "O1")),
            _delete(2, "p1"),  # net live count in O1 is 1 (p2 only)
            _commit(2),
        )

        assert _violations(law, history) == []

    def test_a_bulk_delete_fails_closed(self) -> None:
        # kill_many records no per-row key, so the per-commit fold cannot drop the removed rows — it
        # fails closed rather than check the law against stale state (the final-state oracle handles
        # bulk deletes by querying real committed state).
        history = _history(
            _write(1, _entity("a", "L1", 100)),
            _write(1, _entity("b", "L1", -100)),
            _commit(1),
            _kill_many(2),
            _commit(2),
        )

        with pytest.raises(exc, match="bulk delete"):
            _violations(CONSERVATION, history)

    def test_a_nested_savepoint_exit_is_not_a_commit(self) -> None:
        # The imbalanced debit and the balancing credit share tx1; a NESTED savepoint exits (depth 2)
        # between them. If the fold treated that as a commit it would see -30; correctly it waits for
        # the root exit (depth 1) after both writes, where the sum is 0.
        history = _history(
            _write(1, _entity("a", "L1", 100)),
            _write(1, _entity("b", "L1", -100)),
            _write(1, _entity("a", "L1", 70)),  # debit
            _commit(1, depth=2),  # nested savepoint exit — NOT a commit
            _write(1, _entity("b", "L1", -70)),  # credit
            _commit(1, depth=1),  # the real root commit → sum 0
        )

        assert _violations(CONSERVATION, history) == []


class TestPerCommitCardinality:
    def test_catches_a_double_create_at_the_commit(self) -> None:
        history = _history(
            _write(1, _entity("p1", "O1")),
            _commit(1),
            _write(2, _entity("p2", "O1")),  # second active entry for O1 → count 2
            _commit(2),
        )

        violations = _violations(CARDINALITY, history)

        assert len(violations) == 1
        assert violations[0].invariant == "cardinality"
        assert "O1" in violations[0].message

    def test_where_filter_excludes_out_of_scope_entities(self) -> None:
        history = _history(
            _write(1, _entity("p1", "O1", status="active")),
            _write(1, _entity("p2", "O1", status="voided")),  # not counted (where: active)
            _commit(1),
        )

        assert _violations(CARDINALITY, history) == []

    def test_updates_to_the_same_entity_replace_not_accumulate(self) -> None:
        # The same entity created then updated twice stays ONE entity (materialized by id), so the
        # count is 1, not 3 — updates replace, they do not pile up as new rows.
        history = _history(
            _write(1, _entity("p1", "O1")),
            _commit(1),
            _write(2, _entity("p1", "O1")),  # same id, updated
            _commit(2),
            _write(3, _entity("p1", "O1")),  # same id again
            _commit(3),
        )

        assert _violations(CARDINALITY, history) == []  # count stays 1


class TestPerCommitGuards:
    def test_missing_captured_values_fails_closed(self) -> None:
        # A write CALL event (no captured result) but no result events → capture_values is off.
        call_only = Event(
            seq=1,
            kind="trace",
            at=1.0,
            fields={
                "trace_domain": "document",
                "op": "create",
                "route": ROUTE,
                "phase": "command",
                "tx_id": 1,
                "trace_seq": 1,
                "payload": {"group": "L1", "amount": 5},  # call payload, no result
            },
        )
        history = _history(call_only, _commit(1))

        with pytest.raises(exc, match="capture_values"):
            _violations(CONSERVATION, history)

    def test_no_writes_holds_vacuously(self) -> None:
        assert _violations(CONSERVATION, _history()) == []

    def test_a_richer_where_is_evaluated(self) -> None:
        # A non-scalar where (here ``$and``) is applied via the shared filter evaluator, not rejected:
        # only the entities the where matches count toward the law (the per-commit fold now agrees
        # with the runtime on the full filter DSL).
        law = SystemInvariant(
            name="one_active_per_group",
            read_set=ReadSet(
                spec=ENTRIES,
                scope_keys=("group",),
                where={"$and": [{"$values": {"status": "active"}}]},
            ),
            aggregate=CountAll(),
            holds=lambda n: n <= 1,
        )

        # Two entries in one group, but the where excludes the inactive one — the active count is 1
        # and the law holds. A match-all where would miscount 2 and wrongly flag a violation.
        held = _history(
            _write(1, _entity("p1", "O1", status="active")),
            _write(1, _entity("p2", "O1", status="inactive")),
            _commit(1),
        )
        assert _violations(law, held) == []

        # Two ACTIVE entries in a group break it — the where also counts the rows it matches.
        broken = _history(
            _write(2, _entity("p3", "O2", status="active")),
            _write(2, _entity("p4", "O2", status="active")),
            _commit(2),
        )
        assert _violations(law, broken) != []


# ....................... #
# End to end over a REAL run: a non-atomic transfer commits an intermediate imbalance that a later
# commit heals. v0 (final state) holds; v1 (per-commit fold over the real value-trace) catches it.
# Note: the cluster does not fold the *setup* hook's trace (only node + observe), so all writes the
# fold must see are done in the node — a v1/Cluster bound also noted in the oracle module docstring.

_IDS: dict[str, Any] = {}


async def _open_then_non_atomic_transfer(_node_id: int, ctx: ExecutionContext) -> None:
    # Open a balanced ledger, then debit and credit in SEPARATE committed transactions. Between the
    # debit's commit and the credit's commit the ledger sums to -30 at a committed point, even though
    # the end state nets back to zero. All writes are in the node so the fold sees them.
    async with ctx.tx_ctx.scope("mock"):
        asset = await ctx.document.command(ENTRIES).create(
            _EntryCreate(group="L1", amount=100)
        )
        liability = await ctx.document.command(ENTRIES).create(
            _EntryCreate(group="L1", amount=-100)
        )
        _IDS["asset"], _IDS["liability"] = asset.id, liability.id
    async with ctx.tx_ctx.scope("mock"):
        await ctx.document.command(ENTRIES).update(
            _IDS["asset"], asset.rev, _EntryCreate(group="L1", amount=70)
        )  # debit committed alone → sum -30
    async with ctx.tx_ctx.scope("mock"):
        await ctx.document.command(ENTRIES).update(
            _IDS["liability"], liability.rev, _EntryCreate(group="L1", amount=-70)
        )  # credit → sum 0, healed


async def _batch_create_two_active(_node_id: int, ctx: ExecutionContext) -> None:
    # A BATCH create of two active entries for one order — a cardinality (<= 1) breach. Exercises the
    # value-trace capturing each item of a list result, so the per-commit fold sees both.
    async with ctx.tx_ctx.scope("mock"):
        await ctx.document.command(ENTRIES).create_many(
            [
                _EntryCreate(group="O1", status="active"),
                _EntryCreate(group="O1", status="active"),
            ]
        )


@pytest.fixture(autouse=True)
def _clear_ids() -> None:
    _IDS.clear()


def _cluster(node: Any, observe: Any = None) -> Cluster:
    return Cluster(
        deps=lambda state: MockDepsModule(state=state),
        state_factory=MockState,
        node=node,
        observe=observe,
    )


_CAPTURE = SimulationConfig(
    seeds=range(1), cluster=ClusterConfig(nodes=1), capture_values=True
)


class TestPerCommitEndToEnd:
    def test_v1_catches_a_real_committed_transient_v0_misses(self) -> None:
        v0 = compile_oracle(CONSERVATION)
        v1 = compile_oracle(CONSERVATION, per_commit=True)

        histories = _cluster(
            _open_then_non_atomic_transfer, observe=v0.observe
        ).histories(_CAPTURE)

        # v0 (final state) holds — the transfer nets to zero.
        assert all(check(h, list(v0.invariants)) == [] for h in histories)
        # v1 (per-commit fold over the value-trace) catches the committed mid-transfer imbalance.
        v1_violations = [v for h in histories for v in check(h, list(v1.invariants))]
        assert v1_violations
        assert all(v.invariant == "conservation" for v in v1_violations)

    def test_batch_create_writes_are_folded(self) -> None:
        # Regression: a create_many returns a list; each item must be captured so the fold counts
        # both — otherwise the batch-written entities are silently missed (a false negative).
        v1 = compile_oracle(CARDINALITY, per_commit=True)

        histories = _cluster(_batch_create_two_active).histories(_CAPTURE)
        violations = [v for h in histories for v in check(h, list(v1.invariants))]

        assert violations  # both batch-created entries folded → count 2 → caught
        assert all(v.invariant == "cardinality" for v in violations)
