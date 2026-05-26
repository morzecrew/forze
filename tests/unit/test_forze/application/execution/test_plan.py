"""Unit tests for operation plans and registry dispatch validation."""

import pytest

from forze.base.exceptions import CoreException

from forze.application.contracts.execution import DispatchStep
from forze.application.execution.planning import OperationPlan
from forze.application.execution.registry import OperationRegistry


class TestOperationPlan:
    def test_default_plan_has_empty_scopes(self) -> None:
        plan = OperationPlan()
        assert list(plan.iter_dispatch()) == []

    def test_merge_combines_plans(self) -> None:
        left = OperationPlan()
        right = OperationPlan()
        merged = OperationPlan.merge(left, right)
        assert isinstance(merged, OperationPlan)


class TestOperationRegistryFreeze:
    def test_dispatch_to_missing_target_raises(self) -> None:
        reg = (
            OperationRegistry(handlers={"main": lambda _ctx: None})
            .bind("main")
            .bind_outer()
            .dispatch(
                DispatchStep(id="d1", target="missing", mapper=lambda a, r: r),
            )
            .finish(deep=True)
        )
        with pytest.raises(CoreException, match="Dispatch target"):
            reg.freeze()

    def test_tx_dispatch_without_route_raises_at_freeze(self) -> None:
        reg = (
            OperationRegistry(
                handlers={
                    "main": lambda _ctx: None,
                    "target": lambda _ctx: None,
                },
            )
            .bind("main")
            .bind_tx()
            .dispatch(
                DispatchStep(id="d1", target="target", mapper=lambda a, r: r),
            )
            .finish(deep=True)
        )

        with pytest.raises(CoreException, match="no transaction route"):
            reg.freeze()

    def test_outer_dispatch_without_tx_route_freezes(self) -> None:
        reg = (
            OperationRegistry(
                handlers={
                    "main": lambda _ctx: None,
                    "target": lambda _ctx: None,
                },
            )
            .bind("main")
            .bind_outer()
            .dispatch(
                DispatchStep(id="d1", target="target", mapper=lambda a, r: r),
            )
            .finish(deep=True)
        )

        frozen = reg.freeze()

        assert "main" in frozen.handlers

    def test_registry_merge_detects_handler_conflicts(self) -> None:
        left = OperationRegistry(handlers={"op": lambda _ctx: None})
        right = OperationRegistry(handlers={"op": lambda _ctx: None})
        with pytest.raises(CoreException, match="Conflicting handler"):
            OperationRegistry.merge(left, right)
