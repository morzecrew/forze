"""Unit tests for :class:`RegistryFreezeValidator`."""

import pytest

from forze.application.contracts.execution import DispatchStep
from forze.application.execution.planning import OperationPlan
from forze.application.execution.registry.patch import PlanPatch
from forze.application.execution.registry.resolution import PlanResolution
from forze.application.execution.registry.validation import RegistryFreezeValidator
from forze.base.exceptions import CoreException
from forze.base.primitives import str_key_selector


def _tx_dispatch_plan(*, target: str) -> OperationPlan:
    return (
        OperationPlan()
        .bind_tx()
        .dispatch(
            DispatchStep(id="d1", target=target, mapper=lambda a, r: r),
        )
        .finish(deep=False)
    )


class TestRegistryFreezeValidatorPatches:
    def test_orphan_patch_raises(self) -> None:
        resolution = PlanResolution(
            patches=(
                PlanPatch(
                    selector=str_key_selector.exact("missing"),
                    plan=OperationPlan().bind_tx().set_route("mock").finish(deep=False),
                ),
            ),
        )

        with pytest.raises(CoreException, match="Orphan plan patch"):
            RegistryFreezeValidator.validate_patches(
                {"other": lambda _ctx: None},
                resolution,
            )

    def test_equal_specificity_patch_conflict_raises(self) -> None:
        resolution = PlanResolution(
            patches=(
                PlanPatch(
                    selector=str_key_selector.when(lambda k: k.startswith("o")),
                    plan=OperationPlan().bind_tx().set_route("a").finish(deep=False),
                ),
                PlanPatch(
                    selector=str_key_selector.when(lambda k: "p" in k),
                    plan=OperationPlan().bind_tx().set_route("b").finish(deep=False),
                ),
            ),
        )

        with pytest.raises(CoreException, match="Conflicting plan patches"):
            RegistryFreezeValidator.validate_patches(
                {"op": lambda _ctx: None},
                resolution,
            )


class TestRegistryFreezeValidatorResolvedPlans:
    def test_tx_dispatch_without_route_raises(self) -> None:
        resolution = PlanResolution(
            patches=(
                PlanPatch(
                    selector=str_key_selector.exact("main"),
                    plan=_tx_dispatch_plan(target="target"),
                ),
            ),
        )
        handlers = {
            "main": lambda _ctx: None,
            "target": lambda _ctx: None,
        }

        with pytest.raises(CoreException, match="no transaction route"):
            RegistryFreezeValidator.validate_resolved_plans(handlers, resolution)


class TestRegistryFreezeValidatorDispatchGraph:
    def test_missing_dispatch_target_raises(self) -> None:
        resolution = PlanResolution(
            patches=(
                PlanPatch(
                    selector=str_key_selector.exact("main"),
                    plan=(
                        OperationPlan()
                        .bind_outer()
                        .dispatch(
                            DispatchStep(
                                id="d1",
                                target="missing",
                                mapper=lambda a, r: r,
                            ),
                        )
                        .finish(deep=False)
                    ),
                ),
            ),
        )

        with pytest.raises(CoreException, match="Dispatch target"):
            RegistryFreezeValidator.validate_dispatch_graph(
                {"main": lambda _ctx: None},
                resolution,
            )
