"""Unit tests for :class:`RegistryMerge`."""

import pytest

from forze.application.execution.registry.merge import RegistryMerge
from forze.application.execution.registry.patch import PlanPatch
from forze.application.execution.planning import OperationPlan
from forze.base.exceptions import CoreException
from forze.base.primitives import str_key_selector


def test_merge_combines_disjoint_handlers_plans_and_patches() -> None:
    left = RegistryMerge(
        handlers={"a": lambda _ctx: None},
        plans={"a": OperationPlan()},
        patches=(
            PlanPatch(
                selector=str_key_selector.all_keys(),
                plan=OperationPlan(),
            ),
        ),
    )
    right = RegistryMerge(
        handlers={"b": lambda _ctx: None},
        plans={"b": OperationPlan()},
        patches=(
            PlanPatch(
                selector=str_key_selector.exact("b"),
                plan=OperationPlan(),
            ),
        ),
    )

    merged = RegistryMerge.merge(left, right)

    assert set(merged.handlers) == {"a", "b"}
    assert set(merged.plans) == {"a", "b"}
    assert len(merged.patches) == 2


def test_merge_handler_conflict_raises() -> None:
    a = RegistryMerge(handlers={"op": lambda _ctx: None})
    b = RegistryMerge(handlers={"op": lambda _ctx: None})

    with pytest.raises(CoreException, match="Conflicting handler factories"):
        RegistryMerge.merge(a, b)


def test_merge_plan_conflict_raises() -> None:
    a = RegistryMerge(plans={"op": OperationPlan()})
    b = RegistryMerge(plans={"op": OperationPlan()})

    with pytest.raises(CoreException, match="Conflicting operation plans"):
        RegistryMerge.merge(a, b)


def test_merge_patch_selector_conflict_raises() -> None:
    selector = str_key_selector.all_keys()
    a = RegistryMerge(
        patches=(PlanPatch(selector=selector, plan=OperationPlan()),),
    )
    b = RegistryMerge(
        patches=(PlanPatch(selector=selector, plan=OperationPlan()),),
    )

    with pytest.raises(CoreException, match="Conflicting operation plan patches"):
        RegistryMerge.merge(a, b)
