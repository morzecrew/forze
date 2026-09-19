"""A counterexample report names the property that broke, or it is not a counterexample.

Minimization runs the shrunk workload again to produce the history the report carries. Where a
run leaves state the next one reads — an in-memory store shared across a sweep, most of all —
the candidate that still failed and the replay of it are different runs, and the replay can come
back clean. Reporting that history would hand back an object asserting a violation and listing
none, which no consumer can act on and no `assert report.violations` can read.
"""

from __future__ import annotations

from collections.abc import Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.deps import DepsModule
from forze.application.contracts.execution import Handler
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze_dst import OperationCase, Simulation, SimulationConfig, Strategy
from forze_dst.markers import record_event
from forze_dst.oracle.invariants import Violation, named
from forze_dst.oracle.recorder import History
from forze_mock import MockDepsModule

# ----------------------- #


class Act(BaseModel):
    n: int


def _build(*, stop_after: int) -> Simulation:
    """A workload that records a marker for its first *stop_after* calls and then goes quiet.

    The shape that breaks the replay: the property holds of the runs that happen later, so the
    history the report is built from carries nothing, while the run that failed already did.
    """

    calls = {"n": 0}

    class _Act(Handler[Act, None]):
        async def __call__(self, args: Act) -> None:
            calls["n"] += 1

            if calls["n"] <= stop_after:
                record_event("act", n=args.n)

    def marks_are_a_violation():
        def check(history: History) -> list[Violation]:
            acts = [event for event in history.events if event.kind == "act"]

            if not acts:
                return []

            return [Violation(invariant="any_act", message=f"{len(acts)} acts", events=())]

        return named("any_act", check)

    plan = OperationPlan().bind_tx().set_route("mock").finish(deep=False)
    registry = OperationRegistry(
        handlers={"act": lambda ctx: _Act()},
        plans={"act": plan},
        descriptors={
            "act": OperationDescriptor(input_type=Act, output_type=None, description="act"),
        },
    ).freeze()

    def deps() -> Sequence[DepsModule]:
        return [MockDepsModule()]

    return Simulation(operations=registry, deps=deps, invariants=[marks_are_a_violation()])


def _run(sim: Simulation):
    return sim.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE, count=2, act_count=2, concurrency=1, seeds=range(1)
        ),
        cases=[OperationCase(op="act", inputs=lambda rng: Act(n=rng.randrange(10)))],
    )


# ....................... #


class TestAReportNeverArrivesEmpty:
    def test_a_replay_that_stops_reproducing_still_names_the_violation(self) -> None:
        # Only the first call records, so every run after the first is clean — including the
        # replay the report would otherwise be built from.
        report = _run(_build(stop_after=1))

        assert report is not None
        assert report.violations, "a counterexample that names no violation is not one"
        assert report.violations[0].invariant == "any_act"

    def test_the_history_it_carries_shows_the_violation(self) -> None:
        report = _run(_build(stop_after=1))

        assert report is not None
        assert [e for e in report.history.events if e.kind == "act"]

    def test_a_clean_run_is_still_no_report(self) -> None:
        # The contrast: the fallback must not manufacture a report where nothing failed.
        assert _run(_build(stop_after=0)) is None
