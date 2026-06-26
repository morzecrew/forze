"""The dst_payments double-charge, caught by a *compiled* SystemInvariant oracle (RFC 0012 P3).

The hand-written ``expect("payments", total <= 1)`` in the dst_payments example is exactly the kind of
oracle ``compile_oracle`` generates from a declaration. Here the same scenario (the real registry +
derived scenario) is checked by a declared ``SystemInvariant`` — "at most one payment per order" —
compiled into the DST oracle. Under the faithful (journal) transaction manager the losing payment
rolls back, so the law holds across the seed sweep; under the legacy no-op manager the aborted
payment persists, and the compiled oracle catches the double-charge — grouping by ``order_id`` so it
would catch *any* double-charged order, not a hand-named one.
"""

from __future__ import annotations

from forze.application.contracts.invariants import Count, ReadSet, SystemInvariant
from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.invariants import compile_oracle
from forze_mock import MockDepsModule

from examples.recipes.dst_payments.app import PAYMENT_SPEC, _EVENTS, registry

# ----------------------- #

SINGLE_PAYMENT_PER_ORDER = SystemInvariant(
    name="single_payment_per_order",
    read_set=ReadSet(spec=PAYMENT_SPEC, scope_keys=("order_id",)),
    aggregate=Count(),
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
