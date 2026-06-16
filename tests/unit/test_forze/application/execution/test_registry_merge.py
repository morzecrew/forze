"""Unit tests for :class:`RegistryMerge`."""

import pytest

from forze.application.execution.operations.registry.merge import RegistryMerge
from forze.application.execution.operations.registry.patch import PlanPatch
from forze.application.execution.operations.planning import OperationPlan
from forze.base.exceptions import CoreException
from forze.base.primitives import str_key_selector


def test_merge_combines_disjoint_handlers_plans_and_patches() -> None:
    left = RegistryMerge(
        handlers={"a": lambda _ctx: None},
        plans={"a": OperationPlan()},
        patches=(
            PlanPatch(
                selector=str_key_selector.exact("a"),
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


def test_merge_handler_conflict_raises_configuration_naming_keys() -> None:
    a = RegistryMerge(handlers={"op": lambda _ctx: None})
    b = RegistryMerge(handlers={"op": lambda _ctx: None})

    with pytest.raises(
        CoreException, match=r"duplicate handler factories.*'op'"
    ) as excinfo:
        RegistryMerge.merge(a, b)

    assert excinfo.value.kind.value == "configuration"


def test_merge_plan_conflict_raises_naming_keys() -> None:
    a = RegistryMerge(plans={"op": OperationPlan()})
    b = RegistryMerge(plans={"op": OperationPlan()})

    with pytest.raises(CoreException, match=r"duplicate operation plans.*'op'"):
        RegistryMerge.merge(a, b)


def test_merge_patch_selector_conflict_raises() -> None:
    selector = str_key_selector.all_keys()
    a = RegistryMerge(
        patches=(PlanPatch(selector=selector, plan=OperationPlan()),),
    )
    b = RegistryMerge(
        patches=(PlanPatch(selector=selector, plan=OperationPlan()),),
    )

    with pytest.raises(CoreException, match="duplicate plan patch selector"):
        RegistryMerge.merge(a, b)


def test_merge_handler_conflict_override_last_wins() -> None:
    first = lambda _ctx: None  # noqa: E731
    second = lambda _ctx: None  # noqa: E731

    a = RegistryMerge(handlers={"op": first})
    b = RegistryMerge(handlers={"op": second})

    merged = RegistryMerge.merge(a, b, override=True)

    assert merged.handlers["op"] is second


def test_merge_patch_selector_conflict_override_last_wins() -> None:
    selector = str_key_selector.all_keys()
    first = PlanPatch(selector=selector, plan=OperationPlan())
    second = PlanPatch(selector=selector, plan=OperationPlan())

    merged = RegistryMerge.merge(
        RegistryMerge(patches=(first,)),
        RegistryMerge(patches=(second,)),
        override=True,
    )

    assert merged.patches == (second,)


# ....................... #
# Cross-registry patch reach gate


def _all_keys_patch() -> PlanPatch:
    """
    Create a plan patch that targets all keys.
    
    Returns:
        PlanPatch: A patch with a selector matching all keys and an empty operation plan.
    """
    return PlanPatch(selector=str_key_selector.all_keys(), plan=OperationPlan())


def test_merge_cross_registry_patch_reach_raises_by_default() -> None:
    left = RegistryMerge(
        handlers={"a": lambda _ctx: None},
        patches=(_all_keys_patch(),),
    )
    right = RegistryMerge(handlers={"b": lambda _ctx: None})

    with pytest.raises(CoreException, match="reach operations") as excinfo:
        RegistryMerge.merge(left, right)

    assert excinfo.value.kind.value == "configuration"
    # Names the foreign operation the patch would govern.
    assert "'b'" in str(excinfo.value)


def test_merge_cross_registry_allowed_with_flag() -> None:
    left = RegistryMerge(
        handlers={"a": lambda _ctx: None},
        patches=(_all_keys_patch(),),
    )
    right = RegistryMerge(handlers={"b": lambda _ctx: None})

    merged = RegistryMerge.merge(left, right, cross_registry=True)

    assert set(merged.handlers) == {"a", "b"}
    assert len(merged.patches) == 1


def test_merge_patch_matching_only_own_ops_is_not_flagged() -> None:
    left = RegistryMerge(
        handlers={"a": lambda _ctx: None},
        patches=(PlanPatch(selector=str_key_selector.exact("a"), plan=OperationPlan()),),
    )
    right = RegistryMerge(handlers={"b": lambda _ctx: None})

    merged = RegistryMerge.merge(left, right)

    assert set(merged.handlers) == {"a", "b"}


def test_merge_prior_patch_reaching_later_handler_is_flagged() -> None:
    """The reach is symmetric — order of the colliding part does not matter."""

    left = RegistryMerge(handlers={"a": lambda _ctx: None})
    right = RegistryMerge(
        handlers={"b": lambda _ctx: None},
        patches=(_all_keys_patch(),),
    )

    # right's all_keys patch reaches left's "a".
    with pytest.raises(CoreException, match="reach operations"):
        RegistryMerge.merge(left, right)
