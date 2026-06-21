"""A deadlock is a first-class finding — caught, minimized, and reported, not a crashed run.

A handler that awaits something nothing will ever complete makes the loop quiescent (no ready
work, no pending timer). Instead of aborting the sweep with ``SimulationDeadlock``, the oracle
records it and reports it like any other counterexample.
"""

from __future__ import annotations

import asyncio

import attrs

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze_dst import ModelState, OperationCase, Rule, Scenario, Simulation, SimulationConfig, Strategy
from forze_dst.invariants import check, operation_succeeds
from forze_dst.oracle.recorder import Event, History
from forze_mock import MockDepsModule

# ----------------------- #


@attrs.define(slots=True, kw_only=True)
class _Hang(Handler[None, None]):
    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        # Await an event that is never set: no ready work, no timer → a genuine deadlock.
        await asyncio.Event().wait()


def _hang_sim() -> Simulation:
    registry = OperationRegistry(
        handlers={"hang": lambda ctx: _Hang(ctx=ctx)},
        descriptors={
            "hang": OperationDescriptor(input_type=None, output_type=None, description="x")
        },
    ).freeze()
    # No app invariant — the deadlock must still be found on its own.
    return Simulation(operations=registry, deps=lambda: MockDepsModule(), invariants=[])


_HANG_SCENARIO = Scenario(state=ModelState, act=(Rule(op="hang"),))


# ....................... #


class TestDeadlockFinding:
    def test_scenario_deadlock_is_reported_not_raised(self) -> None:
        # The sweep must not raise SimulationDeadlock; it returns a violation report.
        report = _hang_sim().run(
            SimulationConfig(seeds=range(3), act_count=3, concurrency=3),
            scenario=_HANG_SCENARIO,
        )
        assert report is not None
        assert "no_deadlock" in {v.invariant for v in report.violations}
        assert "deadlock" in report.format()

    def test_op_case_deadlock_is_reported(self) -> None:
        # The op_case strategy catches it through its own run substrate too.
        report = _hang_sim().run(
            SimulationConfig(strategy=Strategy.OP_CASE, seeds=range(2), count=2, concurrency=2),
            cases=[OperationCase(op="hang")],
        )
        assert report is not None
        assert "no_deadlock" in {v.invariant for v in report.violations}

    def test_deadlock_minimizes_to_the_offending_op(self) -> None:
        report = _hang_sim().run(
            SimulationConfig(seeds=range(2), act_count=5, concurrency=5),
            scenario=_HANG_SCENARIO,
        )
        assert report is not None
        # Minimized: the smallest workload that still deadlocks is a single hang.
        assert [op for op, _ in report.workload] == ["hang"]


class TestCheckDetectsDeadlock:
    def test_check_flags_a_recorded_deadlock_event(self) -> None:
        history = History(
            seed=0,
            events=(Event(seq=0, kind="deadlock", at=0.0, fields={"detail": "quiescent"}),),
        )
        violations = check(history, [])  # no invariants — still flagged
        assert [v.invariant for v in violations] == ["no_deadlock"]

    def test_clean_history_has_no_deadlock_violation(self) -> None:
        history = History(
            seed=0,
            events=(Event(seq=0, kind="operation", at=0.0, fields={"op": "x", "outcome": "ok"}),),
        )
        assert check(history, [operation_succeeds("x")]) == []
