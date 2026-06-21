"""Reachability ("sometimes") assertions — the dual of an invariant.

An invariant fails when a property is *ever* violated; a reachability target fails when a
state is *never* reached. These tests pin the primitive's two semantics — per-run
"sometimes" and cross-sweep "must-be-reached" — plus the coverage-sweep integration that
makes a green run mean the dangerous interleavings actually fired, not that they never did.
"""

from __future__ import annotations

import attrs

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze_dst import Simulation, SimulationConfig
from forze_dst.invariants import ReachabilityReport, assess_reachability, no_unexpected_error, reached_labels, sometimes
from forze_dst.markers import reached
from forze_dst.oracle.recorder import Recorder, bind_recorder
from forze_mock import MockDepsModule

# ----------------------- #


def _history_reaching(*labels: str):
    """A standalone recorded history that hit *labels* — no event loop needed."""

    recorder = Recorder(seed=0)
    with bind_recorder(recorder):
        for label in labels:
            reached(label)
    return recorder.history


# ....................... #


class TestMarker:
    def test_reached_is_a_noop_outside_a_recorded_run(self) -> None:
        # No recorder bound → the marker must be a safe no-op (it lives in handler code).
        reached("never-recorded")  # must not raise

    def test_reached_labels_extracts_what_a_run_hit(self) -> None:
        history = _history_reaching("a", "b", "a")
        assert reached_labels(history) == frozenset({"a", "b"})

    def test_reached_carries_context_fields(self) -> None:
        recorder = Recorder(seed=0)
        with bind_recorder(recorder):
            reached("contended", node=3, holder="2")

        event = recorder.history.of_kind("reachability")[0]
        assert event.fields["label"] == "contended"
        assert event.fields["node"] == 3


class TestAssessReachability:
    def test_satisfied_when_every_target_fires_somewhere(self) -> None:
        histories = [_history_reaching("x"), _history_reaching("y")]
        report = assess_reachability(histories, targets={"x", "y"})

        assert report.satisfied
        assert not report.unreached
        assert report.runs == 2

    def test_unreached_target_is_false_confidence(self) -> None:
        # 'z' is declared but no run reached it — the sweep proved nothing about it.
        histories = [_history_reaching("x", "y"), _history_reaching("x")]
        report = assess_reachability(histories, targets={"x", "z"})

        assert not report.satisfied
        assert report.unreached == frozenset({"z"})

    def test_hit_counts_and_extra_labels(self) -> None:
        histories = [_history_reaching("x", "y"), _history_reaching("x")]
        report = assess_reachability(histories, targets={"x"})

        assert report.hits["x"] == 2  # both runs
        assert report.reached == frozenset({"x", "y"})  # incl. the undeclared 'y'
        assert "y" in report.format()  # surfaced as "also reached"

    def test_empty_sweep_leaves_targets_unreached(self) -> None:
        report = assess_reachability([], targets={"x"})
        assert report.runs == 0
        assert report.unreached == frozenset({"x"})


class TestSometimes:
    def test_true_when_any_run_satisfies_the_predicate(self) -> None:
        histories = [_history_reaching("a"), _history_reaching("b")]
        assert sometimes(histories, lambda h: "b" in reached_labels(h))

    def test_false_when_no_run_satisfies(self) -> None:
        histories = [_history_reaching("a"), _history_reaching("b")]
        assert not sometimes(histories, lambda h: "c" in reached_labels(h))


@attrs.define(slots=True, kw_only=True)
class _ReachOp(Handler[None, None]):
    """An op that simply marks a reachability label — every run hits it."""

    ctx: ExecutionContext

    async def __call__(self, _args: None) -> None:
        reached("op-ran")


class TestCoverageSweepIntegration:
    """``Simulation.coverage`` folds reachability across the swept seeds."""

    def _simulation(self) -> Simulation:
        registry = OperationRegistry(
            handlers={"do": lambda ctx: _ReachOp(ctx=ctx)},
            descriptors={
                "do": OperationDescriptor(
                    input_type=None, output_type=None, description="x"
                )
            },
        ).freeze()
        return Simulation(
            operations=registry,
            deps=lambda: MockDepsModule(),
            invariants=[no_unexpected_error()],
        )

    def test_declared_target_is_reached_across_the_sweep(self) -> None:
        stats = self._simulation().coverage(
            SimulationConfig(
                seeds=range(4),
                count=2,
                reachability_targets=frozenset({"op-ran"}),
            )
        )

        assert stats.reachability is not None
        assert stats.reachability.satisfied
        assert stats.reachability.hits["op-ran"] >= 1
        assert "reachability" in stats.format()

    def test_unreachable_target_surfaces_as_unreached(self) -> None:
        stats = self._simulation().coverage(
            SimulationConfig(
                seeds=range(4),
                count=2,
                reachability_targets=frozenset({"never-happens"}),
            )
        )

        assert stats.reachability is not None
        assert not stats.reachability.satisfied
        assert stats.reachability.unreached == frozenset({"never-happens"})

    def test_no_targets_leaves_reachability_absent(self) -> None:
        stats = self._simulation().coverage(
            SimulationConfig(seeds=range(2), count=2, coverage_plateau=0)
        )
        assert stats.reachability is None


class TestReport:
    def test_format_marks_reached_and_unreached(self) -> None:
        report = ReachabilityReport(
            targets=frozenset({"hit", "miss"}),
            hits={"hit": 3},
            runs=5,
        )
        rendered = report.format()
        assert "✓ hit" in rendered
        assert "✗ miss" in rendered
        assert "1/2" in rendered  # one of two targets reached

    def test_zero_count_label_counts_as_unreached(self) -> None:
        # A label present in `hits` but with count 0 is *not* reached and stays unreached.
        report = ReachabilityReport(
            targets=frozenset({"hit", "stale"}),
            hits={"hit": 2, "stale": 0},
            runs=2,
        )
        assert report.reached == frozenset({"hit"})  # zero-count 'stale' excluded
        assert report.unreached == frozenset({"stale"})
        assert not report.satisfied

    def test_all_targets_reached_is_satisfied(self) -> None:
        report = ReachabilityReport(
            targets=frozenset({"hit"}),
            hits={"hit": 1},
            runs=1,
        )
        assert report.satisfied
        assert report.unreached == frozenset()
        # No undeclared labels → no "also reached" line.
        assert "also reached" not in report.format()
