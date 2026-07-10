"""The DST moat: a kit-declared aggregate's invariant holds under simulated concurrency.

The contrast is the proof (mirrors the payments example): under the *same* deterministic schedule
and the *same* compiled oracle, a **bare** registry with no enforcement double-books the sprint's
capacity (DST reports a violation), while the **kit-composed** registry — whose declaration folded
preventive enforcement into its write ops — holds. So the composition is what makes the slice
DST-verifiable, and DST confirms it, not just a single-threaded check.
"""

from __future__ import annotations

from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.oracle import compile_oracle
from forze_kits.aggregates.document import build_document_registry
from forze_mock import MockDepsModule

from examples.recipes.aggregate_kit_dst.app import (
    SPRINT_CAPACITY,
    TICKET_SPEC,
    TICKETS,
    simulation,
)

# ----------------------- #

_CONFIG = SimulationConfig(
    strategy=Strategy.SCENARIO, act_count=8, concurrency=4, seeds=range(10)
)


def _run(operations) -> object:
    oracle = compile_oracle(SPRINT_CAPACITY)
    sim = Simulation(
        operations=operations,
        deps=lambda: MockDepsModule(),
        observe=oracle.observe,
        invariants=[*oracle.invariants],
    )
    return sim.run(_CONFIG, scenario=sim.derive_scenario())


class TestKitSliceIsDstVerifiable:
    def test_bare_registry_double_books_the_sprint(self) -> None:
        # No enforcement: concurrent creates push the sprint past its capacity — DST catches it.
        report = _run(build_document_registry(TICKET_SPEC).freeze())
        assert report is not None  # a violation is reported

    def test_kit_enforced_slice_holds_under_concurrency(self) -> None:
        # The kit folded preventive enforcement into the write ops; the same schedule finds nothing.
        report = simulation.run(_CONFIG, scenario=simulation.derive_scenario())
        assert report is None  # no violation — the declared invariant holds under interleaving

    def test_kit_registry_matches_the_example_simulation(self) -> None:
        # The simulation drives exactly the kit's composed registry (not a hand-built one).
        assert simulation.operations is not None
        assert TICKETS.registry(tx_route="mock").fingerprint() == simulation.operations.fingerprint()
