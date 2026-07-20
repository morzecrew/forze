"""The dst_payments double-charge, caught by a *compiled* SystemInvariant oracle.

The hand-written ``expect("payments", total <= 1)`` in the dst_payments example is exactly the kind of
oracle ``compile_oracle`` generates from a declaration. Here the same scenario (the real registry +
derived scenario) is checked by a declared ``SystemInvariant`` — "at most one payment per order" —
compiled into the DST oracle. Under the faithful (journal) transaction manager the losing payment
rolls back, so the law holds across the seed sweep; under the legacy no-op manager the aborted
payment persists, and the compiled oracle catches the double-charge — grouping by ``order_id`` so it
would catch *any* double-charged order, not a hand-named one.
"""

from __future__ import annotations

from examples.recipes.dst_payments.app import _EVENTS, PAYMENT_SPEC, registry
from forze.application.contracts.invariants import CountAll, ReadSet, SystemInvariant
from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.invariants import compile_oracle
from forze_mock import MockDepsModule

# ----------------------- #

SINGLE_PAYMENT_PER_ORDER = SystemInvariant(
    name="single_payment_per_order",
    read_set=ReadSet(spec=PAYMENT_SPEC, scope_keys=("order_id",)),
    aggregate=CountAll(),
    holds=lambda n: n <= 1,
)


def _simulation(transactions: str) -> Simulation:
    oracle = compile_oracle(SINGLE_PAYMENT_PER_ORDER)
    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(domain_events=_EVENTS, transactions=transactions),
        observe=oracle.observe,
        invariants=[*oracle.invariants],
    )


def _config(seeds: range) -> SimulationConfig:
    return SimulationConfig(
        strategy=Strategy.SCENARIO, act_count=4, concurrency=4, seeds=seeds
    )


# ----------------------- #


class TestDstPaymentsCompiledOracle:
    def test_faithful_tx_holds_the_cardinality_law(self) -> None:
        # Journal (faithful) tx: the loser's rev-conflict rolls its payment back, so every order has
        # at most one payment — the compiled oracle reports no violation across the sweep.
        sim = _simulation(transactions="journal")  # the faithful (default) manager
        report = sim.run(_config(range(8)), scenario=sim.derive_scenario())

        assert report is None

    def test_no_op_tx_oracle_catches_the_double_charge(self) -> None:
        # No-op tx: the aborted payment is not rolled back, so an order ends with two payments —
        # the declared, compiled SystemInvariant oracle catches it (the same bug the hand-written
        # expect() in the example catches).
        sim = _simulation(transactions="none")
        report = sim.run(_config(range(5)), scenario=sim.derive_scenario())

        assert report is not None
        assert report.violations[0].invariant == "single_payment_per_order"


# ....................... #
# The v1 per-commit oracle over a REAL simulation trace (capture_values on) — proving the trace fold
# reads an actual run end to end, not only synthesized histories.


def _per_commit_simulation(transactions: str) -> Simulation:
    oracle = compile_oracle(SINGLE_PAYMENT_PER_ORDER, per_commit=True)
    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(domain_events=_EVENTS, transactions=transactions),
        observe=oracle.observe,  # a no-op for v1; the invariants read the folded trace
        invariants=[*oracle.invariants],
    )


def _capture_config(seeds: range) -> SimulationConfig:
    return SimulationConfig(
        strategy=Strategy.SCENARIO,
        act_count=4,
        concurrency=4,
        seeds=seeds,
        capture_values=True,  # v1 reconstructs entities from captured write results
    )


class TestDstPaymentsPerCommitOracle:
    def test_faithful_tx_holds_per_commit(self) -> None:
        # The loser's payment rolls back (its transaction never commits), so no committed point ever
        # has two payments for an order — the per-commit fold reports nothing.
        sim = _per_commit_simulation(transactions="journal")
        report = sim.run(_capture_config(range(8)), scenario=sim.derive_scenario())

        assert report is None

    def test_per_commit_reconstructs_the_faithful_view_not_the_unfaithful_state(self) -> None:
        # The trust boundary, made concrete. The per-commit fold reconstructs the
        # FAITHFUL world — a rolled-back transaction undoes its writes. The no-op manager is the
        # deliberately-*unfaithful* one: the loser's transaction raises (its scope records a
        # rollback) yet its payment is NOT actually undone, so the real state double-charges. v1
        # trusts the trace's rollback signal and reports the faithful answer (one payment, no
        # violation); catching an unfaithful backend is v0's + conformance's job, not v1's. So the
        # per-commit fold here reports nothing — it is only as sound as the backend's conformance.
        sim = _per_commit_simulation(transactions="none")
        report = sim.run(_capture_config(range(5)), scenario=sim.derive_scenario())

        assert report is None
