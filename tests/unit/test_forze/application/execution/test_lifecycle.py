"""Tests for forze.application.execution.lifecycle."""

import pytest

from forze.application.contracts.execution import noop_lifecycle_hook
from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle import LifecyclePlan, LifecycleStep
from forze_mock import MockDepsModule, MockState

# ----------------------- #


@pytest.fixture
def ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())


class TestNoopLifecycleHook:
    @pytest.mark.asyncio
    async def test_noop_lifecycle_hook_returns_none(self, ctx: ExecutionContext) -> None:
        result = await noop_lifecycle_hook(ctx)
        assert result is None


class TestLifecycleStep:
    def test_defaults_to_noop(self) -> None:
        step = LifecycleStep(id="test")
        assert step.startup is noop_lifecycle_hook
        assert step.shutdown is noop_lifecycle_hook

    def test_with_custom_hooks(self) -> None:
        async def up(ctx: ExecutionContext) -> None:
            pass

        async def down(ctx: ExecutionContext) -> None:
            pass

        step = LifecycleStep(id="custom", startup=up, shutdown=down)
        assert step.startup is up
        assert step.shutdown is down


class TestLifecyclePlan:
    @pytest.mark.asyncio
    async def test_startup_and_shutdown_run_in_order(self, ctx: ExecutionContext) -> None:
        order: list[str] = []

        async def up(_ctx: ExecutionContext) -> None:
            order.append("up")

        async def down(_ctx: ExecutionContext) -> None:
            order.append("down")

        plan = LifecyclePlan.from_steps(
            LifecycleStep(id="s", startup=up, shutdown=down),
        )
        await plan.startup(ctx)
        await plan.shutdown(ctx)
        assert order == ["up", "down"]
