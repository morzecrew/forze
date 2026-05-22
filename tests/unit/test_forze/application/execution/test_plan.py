"""Unit tests for operation plans and registry dispatch validation."""

import pytest

from forze.application.contracts.execution import DispatchStep
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.planning import OperationPlan
from forze.application.execution.registry import OperationRegistry
from forze.base.errors import CoreError


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
        with pytest.raises(CoreError, match="Dispatch target"):
            reg.freeze()

    def test_registry_merge_detects_handler_conflicts(self) -> None:
        left = OperationRegistry(handlers={"op": lambda _ctx: None})
        right = OperationRegistry(handlers={"op": lambda _ctx: None})
        with pytest.raises(CoreError, match="Conflicting handler"):
            OperationRegistry.merge(left, right)
