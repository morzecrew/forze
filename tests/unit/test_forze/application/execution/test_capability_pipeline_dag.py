"""Tests for the public stage DAG authoring API."""

from __future__ import annotations

import pytest

from forze.application.execution import UsecaseRegistry
from forze.application.execution.capabilities import schedule_capability_specs
from forze.application.execution.engine import Stage
from forze.application.execution.plan import DagNode, PlanDag
from forze.application.execution.plan.dag import dag_completion_key
from forze.base.errors import CoreError


def _guard_factory(_name: str):
    def factory(_ctx):
        async def guard(_args):
            return None

        return guard

    return factory


def _success_hook_factory(_name: str):
    def factory(_ctx):
        async def hook(_args, _result):
            return None

        return hook

    return factory


def test_before_dag_orders_nodes_under_scheduler() -> None:
    dag = PlanDag(
        nodes=(
            DagNode(id="consumer", factory=_guard_factory("consumer"), priority=100),
            DagNode(id="provider", factory=_guard_factory("provider"), priority=0),
        ),
        edges=(("provider", "consumer"),),
    )

    reg = UsecaseRegistry().before_dag("op", dag)
    specs = reg._stages["op"].specs(Stage.before)
    ordered = schedule_capability_specs(specs, stage=Stage.before.value)

    assert [spec.step_label for spec in ordered] == ["provider", "consumer"]


def test_before_dag_preserves_domain_caps() -> None:
    dag = PlanDag(
        nodes=(
            DagNode(
                id="b",
                factory=_guard_factory("b"),
                requires=frozenset({"domain.k"}),
                provides=frozenset({"domain.bout"}),
            ),
            DagNode(
                id="a",
                factory=_guard_factory("a"),
                provides=frozenset({"domain.k"}),
            ),
        ),
        edges=(("a", "b"),),
    )

    reg = UsecaseRegistry().before_dag("op", dag)
    by_label = {spec.step_label: spec for spec in reg._stages["op"].specs(Stage.before)}

    assert by_label["a"].requires == frozenset()
    assert "domain.k" in by_label["a"].provides
    assert "domain.k" in by_label["b"].requires
    assert "domain.bout" in by_label["b"].provides
    assert any(key.startswith("dag.") for key in by_label["a"].provides)
    assert any(key.startswith("dag.") for key in by_label["b"].provides)


def test_before_dag_cycle_raises() -> None:
    dag = PlanDag(
        nodes=(
            DagNode(id="a", factory=_guard_factory("a")),
            DagNode(id="b", factory=_guard_factory("b")),
        ),
        edges=(("a", "b"), ("b", "a")),
    )

    with pytest.raises(CoreError, match="directed cycle"):
        UsecaseRegistry().before_dag("op", dag)


def test_before_dag_unknown_edge_endpoint_raises() -> None:
    dag = PlanDag(
        nodes=(DagNode(id="only", factory=_guard_factory("only")),),
        edges=(("only", "missing"),),
    )

    with pytest.raises(CoreError, match="Unknown plan DAG node id 'missing'"):
        UsecaseRegistry().before_dag("op", dag)


def test_before_dag_duplicate_node_id_raises() -> None:
    guard = _guard_factory("guard")
    dag = PlanDag(
        nodes=(
            DagNode(id="dup", factory=guard),
            DagNode(id="dup", factory=guard),
        ),
    )

    with pytest.raises(CoreError, match="Duplicate plan DAG node id 'dup'"):
        UsecaseRegistry().before_dag("op", dag)


def test_before_dag_self_loop_raises() -> None:
    dag = PlanDag(
        nodes=(DagNode(id="a", factory=_guard_factory("a")),),
        edges=(("a", "a"),),
    )

    with pytest.raises(CoreError, match="self-loop"):
        UsecaseRegistry().before_dag("op", dag)


def test_before_dag_reserved_completion_key_overlap_raises() -> None:
    reserved = dag_completion_key("before.op.0", "b")
    dag = PlanDag(
        nodes=(
            DagNode(
                id="a", factory=_guard_factory("a"), provides=frozenset({reserved})
            ),
            DagNode(id="b", factory=_guard_factory("b")),
        ),
    )

    with pytest.raises(CoreError, match="reserved completion keys"):
        UsecaseRegistry().before_dag("op", dag)


def test_after_success_dag_smoke() -> None:
    dag = PlanDag(
        nodes=(
            DagNode(id="consumer", factory=_success_hook_factory("consumer")),
            DagNode(id="provider", factory=_success_hook_factory("provider")),
        ),
        edges=(("provider", "consumer"),),
    )

    reg = UsecaseRegistry().after_success_dag("op", dag)
    specs = reg._stages["op"].specs(Stage.after_success)

    assert len(specs) == 2
    assert all(spec.requires or spec.provides for spec in specs)


def test_after_commit_dag_smoke() -> None:
    dag = PlanDag(
        nodes=(DagNode(id="publish", factory=_success_hook_factory("publish")),),
    )

    reg = UsecaseRegistry().tx("op", route="mock").after_commit_dag("op", dag)

    assert len(reg._stages["op"].specs(Stage.after_commit)) == 1
