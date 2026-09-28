"""Unit tests for operation plans and registry dispatch validation."""

import pytest

from forze.application.contracts.execution import DispatchStep
from forze.application.execution.operations.planning import OperationPlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException


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
        with pytest.raises(CoreException, match=r"duplicate handler factories.*'op'"):
            OperationRegistry.merge(left, right)


class TestReenteringAScope:
    """A binding that comes back to a scope adds to it; it never replaces what was bound."""

    async def test_steps_bound_before_survive(self) -> None:
        from forze.application.contracts.execution import BeforeStep, OnSuccessStep
        from forze.application.execution.operations import run_operation
        from forze.testing import context_from_modules
        from forze_mock import MockDepsModule

        seen: list[str] = []

        def _hook(label: str):
            def _factory(ctx):
                async def _hook_call(*_args):
                    seen.append(label)

                return _hook_call

            return _factory

        async def _handler(args):
            return args

        reg = (
            OperationRegistry(handlers={"op": lambda ctx: _handler})
            .bind("op")
            .bind_outer()
            .before(BeforeStep(id="guard", factory=_hook("guard")))
            .bind_tx()
            .set_route("mock")
            .finish()
            .bind_outer()
            .before(BeforeStep(id="later", factory=_hook("later")))
            .finish()
            .bind_tx()
            .on_success(OnSuccessStep(id="in_tx", factory=_hook("in_tx")))
            .finish(deep=True)
            .freeze()
        )

        # Before the fix the second bind_outer() dropped the guard, and the second bind_tx()
        # dropped the route, so this raised "transaction stages … but no transaction route".
        await run_operation(reg, "op", None, context_from_modules(MockDepsModule()))

        assert sorted(seen) == ["guard", "in_tx", "later"]
