"""Tests for forze.application.execution.lifecycle."""

import pytest

from forze.application.execution import ExecutionContext
from forze.application.execution.lifecycle import (
    LifecyclePlan,
    LifecycleStep,
    noop_hook,
)
from forze.base.errors import CoreError
from forze_mock import MockDepsModule, MockState

# ----------------------- #


@pytest.fixture
def ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())


class TestNoopHook:
    async def test_noop_hook_returns_none(self, ctx: ExecutionContext) -> None:
        result = await noop_hook(ctx)
        assert result is None


class TestLifecycleStep:
    def test_defaults_to_noop(self) -> None:
        step = LifecycleStep(name="test")
        assert step.startup is noop_hook
        assert step.shutdown is noop_hook

    def test_with_custom_hooks(self) -> None:
        async def up(ctx: ExecutionContext) -> None:
            pass

        async def down(ctx: ExecutionContext) -> None:
            pass

        step = LifecycleStep(name="custom", startup=up, shutdown=down)
        assert step.startup is up
        assert step.shutdown is down


class TestLifecyclePlan:
    def test_from_steps(self) -> None:
        s1 = LifecycleStep(name="a")
        s2 = LifecycleStep(name="b")
        plan = LifecyclePlan.from_steps(s1, s2)
        assert len(plan.steps) == 2

    def test_from_steps_name_collision_raises(self) -> None:
        s1 = LifecycleStep(name="dup")
        s2 = LifecycleStep(name="dup")
        with pytest.raises(CoreError, match="collision"):
            LifecyclePlan.from_steps(s1, s2)

    def test_with_steps(self) -> None:
        s1 = LifecycleStep(name="a")
        s2 = LifecycleStep(name="b")
        plan = LifecyclePlan.from_steps(s1)
        plan2 = plan.with_steps(s2)
        assert len(plan2.steps) == 2

    def test_with_steps_collision_raises(self) -> None:
        s1 = LifecycleStep(name="a")
        plan = LifecyclePlan.from_steps(s1)
        with pytest.raises(CoreError, match="collision"):
            plan.with_steps(LifecycleStep(name="a"))

    async def test_startup_runs_in_order(self, ctx: ExecutionContext) -> None:
        order: list[str] = []

        async def hook_a(c: ExecutionContext) -> None:
            order.append("a")

        async def hook_b(c: ExecutionContext) -> None:
            order.append("b")

        plan = LifecyclePlan.from_steps(
            LifecycleStep(name="a", startup=hook_a),
            LifecycleStep(name="b", startup=hook_b),
        )
        await plan.startup(ctx)
        assert order == ["a", "b"]

    async def test_shutdown_runs_in_reverse(self, ctx: ExecutionContext) -> None:
        order: list[str] = []

        async def down_a(c: ExecutionContext) -> None:
            order.append("a")

        async def down_b(c: ExecutionContext) -> None:
            order.append("b")

        plan = LifecyclePlan.from_steps(
            LifecycleStep(name="a", shutdown=down_a),
            LifecycleStep(name="b", shutdown=down_b),
        )
        await plan.shutdown(ctx)
        assert order == ["b", "a"]

    async def test_startup_failure_shuts_down_executed(
        self, ctx: ExecutionContext
    ) -> None:
        shutdown_log: list[str] = []

        async def up_ok(c: ExecutionContext) -> None:
            pass

        async def up_fail(c: ExecutionContext) -> None:
            raise RuntimeError("fail")

        async def down_a(c: ExecutionContext) -> None:
            shutdown_log.append("a")

        async def down_b(c: ExecutionContext) -> None:
            shutdown_log.append("b")

        plan = LifecyclePlan.from_steps(
            LifecycleStep(name="a", startup=up_ok, shutdown=down_a),
            LifecycleStep(name="b", startup=up_fail, shutdown=down_b),
        )
        with pytest.raises(RuntimeError):
            await plan.startup(ctx)

        assert "a" in shutdown_log
        assert "b" not in shutdown_log

    async def test_shutdown_swallows_exceptions(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []

        async def down_fail(c: ExecutionContext) -> None:
            order.append("fail")
            raise RuntimeError("oops")

        async def down_ok(c: ExecutionContext) -> None:
            order.append("ok")

        plan = LifecyclePlan.from_steps(
            LifecycleStep(name="a", shutdown=down_ok),
            LifecycleStep(name="b", shutdown=down_fail),
        )
        await plan.shutdown(ctx)
        assert order == ["fail", "ok"]

    async def test_empty_plan_startup_and_shutdown(
        self, ctx: ExecutionContext
    ) -> None:
        plan = LifecyclePlan()
        await plan.startup(ctx)
        await plan.shutdown(ctx)
