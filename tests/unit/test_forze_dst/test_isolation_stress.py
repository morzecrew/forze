"""The isolation oracle *pays off* — a generated, contended workload that actually fires it.

The serializability oracle (`serializable(complete=True)`, RFC 0013 P1+P2) can detect anti-dependency
cycles and predicate phantoms, but only if a workload *produces* them. These tests drive the reusable
contended stress scenario (`tests.support.dst_isolation_stress`) and prove the end-to-end payoff:

* at ``SNAPSHOT`` the oracle **catches** a generated write-skew cycle and a generated predicate
  phantom — anomalies a random workload almost never surfaces;
* at ``SERIALIZABLE`` the same workload is **clean** (the mock's SSI prevents them), and the sweep is
  **non-vacuous** (``had_isolation_conflict`` confirms it actually stressed isolation);
* a non-contended foil is correctly reported vacuous — so a green result over it would mean nothing;
* read-modify-write is safe at every level (rev-OCC aborts the stale writer — no lost update);
* `isolation_oracle_for` ties a *declared* level to the guarantee it actually checks.
"""

from __future__ import annotations

import pytest

from forze.application.contracts.transaction import IsolationLevel
from forze.base.exceptions import exc
from forze_dst import Simulation, SimulationConfig
from forze_dst.invariants import (
    had_isolation_conflict,
    isolation_oracle_for,
    serializable,
)
from forze_dst.oracle.recorder import History
from forze_mock import MockDepsModule
from tests.support.dst_isolation_stress import (
    disjoint_scenario,
    stress_registry,
    stress_scenario,
)

pytestmark = pytest.mark.unit

_SI = IsolationLevel.SNAPSHOT
_SER = IsolationLevel.SERIALIZABLE


def _run(level, invariant, scenario, *, seeds, act_count=16):  # type: ignore[no-untyped-def]
    """Run the stress workload at *level*, checking *invariant*; return ``(report, histories)``."""

    histories: list[History] = []

    def _grab(history: History) -> list:  # type: ignore[type-arg]
        histories.append(history)
        return []

    report = Simulation(
        operations=stress_registry(level),
        deps=lambda: MockDepsModule(),
        invariants=[invariant, _grab],
    ).run(
        SimulationConfig(
            seeds=range(seeds), act_count=act_count, concurrency=4, capture_values=True
        ),
        scenario=scenario,
    )
    return report, histories


# ----------------------- #
# The payoff: the oracle catches generated anomalies at SNAPSHOT, is clean at SERIALIZABLE.


class TestOraclePayoff:
    def test_write_skew_cycle_caught_at_snapshot(self) -> None:
        report, _ = _run(
            _SI,
            serializable(complete=True),
            stress_scenario(shapes=("write_skew",)),
            seeds=96,
        )
        assert report is not None, "the oracle did not catch the generated write skew"
        assert "dependency cycle" in report.violations[0].message

    def test_predicate_phantom_caught_at_snapshot(self) -> None:
        # The P2 headline: a generated scan-then-insert produces a phantom the oracle catches via a
        # PREDICATE edge — not a hand-scripted battery case.
        report, _ = _run(
            _SI,
            serializable(complete=True),
            stress_scenario(shapes=("scan_insert",)),
            seeds=96,
        )
        assert report is not None, "the oracle did not catch the generated phantom"
        assert "predicate" in report.violations[0].message

    def test_serializable_is_clean_and_non_vacuous(self) -> None:
        # The same contended workload at SERIALIZABLE: the mock's SSI prevents every anomaly, so the
        # sweep is clean — AND it genuinely stressed isolation (non-vacuous), so the green means something.
        report, histories = _run(
            _SER, serializable(complete=True), stress_scenario(), seeds=48
        )
        assert report is None, "SERIALIZABLE must prevent the anomalies"
        assert any(had_isolation_conflict(history) for history in histories), (
            "the SERIALIZABLE sweep was vacuous — it never produced a conflict, so 'clean' is empty"
        )

    def test_read_modify_write_is_safe_at_every_level(self) -> None:
        # RMW on one key is a lost-update shape, but rev-OCC aborts the stale writer at every level, so
        # no anomaly survives — the oracle correctly finds nothing even at SNAPSHOT. Note: unlike the
        # SERIALIZABLE-clean test above, this green is NOT non-vacuous by `had_isolation_conflict` — the
        # abort that makes RMW safe leaves ≤1 committed writer per key, so no conflict pair remains. The
        # test's teeth are against an OCC *regression*: if rev-OCC stopped aborting, both writes would
        # commit and the oracle would fire (a lost-update ww/rw cycle).
        report, _ = _run(
            _SI, serializable(complete=True), stress_scenario(shapes=("rmw",)), seeds=48
        )
        assert report is None


# ....................... #
# The non-vacuity signal discriminates a contended workload from a disjoint one.


class TestNonVacuity:
    def test_contended_workload_is_flagged_as_stressing_isolation(self) -> None:
        _, histories = _run(_SER, serializable(complete=True), stress_scenario(), seeds=32)
        assert any(had_isolation_conflict(history) for history in histories)

    def test_disjoint_workload_is_reported_vacuous(self) -> None:
        # Each act commits a tx that creates a fresh (unique-key) cell: concurrent committed
        # transactions exist, but none conflict — so a clean oracle over this is meaningless, and
        # had_isolation_conflict must say so.
        _, histories = _run(
            _SI, serializable(complete=True), disjoint_scenario(), seeds=24
        )
        assert histories, "the foil produced no runs"
        assert not any(had_isolation_conflict(history) for history in histories), (
            "disjoint single-key creates must not register as an isolation conflict"
        )


# ....................... #
# A3 — a declared isolation level is checked by the oracle for exactly that level.


class TestDeclaredLevelOracle:
    def test_snapshot_level_gets_the_snapshot_guarantee_not_serializability(self) -> None:
        # An operation declared at SNAPSHOT: the SNAPSHOT oracle holds (no lost update), but the
        # SERIALIZABLE oracle does NOT — you get exactly the level you declared, no more.
        scenario = stress_scenario(shapes=("write_skew",))

        snapshot_report, _ = _run(
            _SI, isolation_oracle_for(_SI), scenario, seeds=48
        )
        assert snapshot_report is None, "SNAPSHOT's own guarantee (no lost update) must hold"

        serializable_report, _ = _run(
            _SI, isolation_oracle_for(_SER), scenario, seeds=96
        )
        assert serializable_report is not None, (
            "the SERIALIZABLE oracle must catch the write skew a SNAPSHOT run permits"
        )

    def test_serializable_level_gets_serializability(self) -> None:
        report, _ = _run(_SER, isolation_oracle_for(_SER), stress_scenario(), seeds=48)
        assert report is None

    def test_read_committed_has_no_graph_oracle(self) -> None:
        with pytest.raises(exc, match="no_isolation_oracle_for_level"):
            isolation_oracle_for(IsolationLevel.READ_COMMITTED)
