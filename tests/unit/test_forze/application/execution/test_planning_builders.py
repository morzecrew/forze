"""Tests for execution planning builders."""

from __future__ import annotations

import pytest

from forze.application.contracts.execution import GraphStep, LifecycleStep
from forze.application.contracts.execution.builders import (
    steps_graph_from_sequence,
    steps_pipe_from_sequence,
)
from forze.application.execution.planning.builders import (
    lifecycle_graph_from_sequence,
    lifecycle_steps_from_sequence,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import AbstractSequence

# ----------------------- #


def _graph_seq(*steps: GraphStep) -> AbstractSequence[GraphStep]:
    return AbstractSequence(items=steps)


class TestGraphFromSequence:
    def test_builds_waves_from_capabilities(self) -> None:
        a = GraphStep(id="a", priority=0, provides=("db",))
        b = GraphStep(id="b", priority=1, requires=("db",))
        graph = steps_graph_from_sequence(_graph_seq(a, b))

        assert set(graph.steps) == {"a", "b"}
        assert graph.waves == (("a",), ("b",))

    def test_depends_on_edges(self) -> None:
        a = GraphStep(id="a", priority=0)
        b = GraphStep(id="b", priority=1, depends_on=("a",))
        graph = steps_graph_from_sequence(_graph_seq(a, b))

        assert graph.waves == (("a",), ("b",))

    def test_duplicate_step_id_raises(self) -> None:
        a = GraphStep(id="dup", priority=0)
        b = GraphStep(id="dup", priority=1)

        with pytest.raises(CoreException, match="not unique"):
            steps_graph_from_sequence(_graph_seq(a, b))

    def test_duplicate_capability_provider_raises(self) -> None:
        a = GraphStep(id="a", provides=("x",))
        b = GraphStep(id="b", provides=("x",))

        with pytest.raises(CoreException, match="more than one step"):
            steps_graph_from_sequence(_graph_seq(a, b))

    def test_missing_depends_on_step_raises(self) -> None:
        step = GraphStep(id="b", depends_on=("missing",))

        with pytest.raises(CoreException, match="not found"):
            steps_graph_from_sequence(_graph_seq(step))

    def test_missing_required_capability_raises(self) -> None:
        step = GraphStep(id="b", requires=("missing",))

        with pytest.raises(CoreException, match="no step provides"):
            steps_graph_from_sequence(_graph_seq(step))


class TestLifecycleGraphFromSequence:
    def test_builds_waves_from_capabilities(self) -> None:
        pool = LifecycleStep(id="pool", provides=("postgres.client",))
        warmup = LifecycleStep(id="warmup", requires=("postgres.client",))
        graph = lifecycle_graph_from_sequence((warmup, pool))

        assert graph.waves == (("pool",), ("warmup",))


class TestLifecycleStepsFromSequence:
    def test_orders_by_required_capability(self) -> None:
        pool = LifecycleStep(id="pool", provides=("postgres.client",))
        warmup = LifecycleStep(id="warmup", requires=("postgres.client",))
        ordered = lifecycle_steps_from_sequence((warmup, pool))

        assert [s.id for s in ordered] == ["pool", "warmup"]

    def test_preserves_registration_order_without_edges(self) -> None:
        a = LifecycleStep(id="a")
        b = LifecycleStep(id="b")
        c = LifecycleStep(id="c")

        ordered = lifecycle_steps_from_sequence((a, b, c))

        assert [s.id for s in ordered] == ["a", "b", "c"]

    def test_higher_priority_runs_first_within_wave(self) -> None:
        low = LifecycleStep(id="low", priority=0)
        high = LifecycleStep(id="high", priority=10)

        ordered = lifecycle_steps_from_sequence((low, high))

        assert [s.id for s in ordered] == ["high", "low"]


class TestPipeFromSequence:
    def test_sorts_by_priority(self) -> None:
        from forze.application.contracts.execution import Step

        low = Step(id="low", priority=10)
        high = Step(id="high", priority=0)
        pipe = steps_pipe_from_sequence(AbstractSequence(items=(low, high)))

        assert [s.id for s in pipe.steps] == ["high", "low"]
