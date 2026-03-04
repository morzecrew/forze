"""Unit tests for forze.application.execution.lifecycle."""

import pytest

from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.lifecycle import (
    LifecyclePlan,
    LifecycleStep,
    noop_hook,
)

# ----------------------- #


class TestLifecycleStep:
    """Tests for LifecycleStep."""

    def test_default_hooks_are_noop(self) -> None:
        step = LifecycleStep(name="test")
        assert step.startup is noop_hook
        assert step.shutdown is noop_hook

    def test_custom_startup_and_shutdown(self) -> None:
        async def my_startup(ctx):
            pass

        async def my_shutdown(ctx):
            pass

        step = LifecycleStep(name="x", startup=my_startup, shutdown=my_shutdown)
        assert step.startup is my_startup
        assert step.shutdown is my_shutdown


class TestLifecyclePlan:
    """Tests for LifecyclePlan."""

    def test_from_steps_creates_plan(self) -> None:
        step = LifecycleStep(name="a")
        plan = LifecyclePlan.from_steps(step)
        assert len(plan.steps) == 1
        assert plan.steps[0].name == "a"

    def test_from_steps_name_collision_raises(self) -> None:
        from forze.base.errors import CoreError

        step_a = LifecycleStep(name="dup")
        step_b = LifecycleStep(name="dup")
        with pytest.raises(CoreError, match="name collision"):
            LifecyclePlan.from_steps(step_a, step_b)

    def test_with_steps_appends(self) -> None:
        plan = LifecyclePlan.from_steps(LifecycleStep(name="a"))
        new = plan.with_steps(LifecycleStep(name="b"))
        assert len(new.steps) == 2
        assert new.steps[0].name == "a"
        assert new.steps[1].name == "b"

    def test_with_steps_collision_raises(self) -> None:
        from forze.base.errors import CoreError

        plan = LifecyclePlan.from_steps(LifecycleStep(name="a"))
        with pytest.raises(CoreError, match="name collision"):
            plan.with_steps(LifecycleStep(name="a"))

    @pytest.mark.asyncio
    async def test_startup_runs_in_order(self) -> None:
        order: list[str] = []

        async def start_a(ctx):
            order.append("a")

        async def start_b(ctx):
            order.append("b")

        step_a = LifecycleStep(name="a", startup=start_a)
        step_b = LifecycleStep(name="b", startup=start_b)
        plan = LifecyclePlan.from_steps(step_a, step_b)
        ctx = ExecutionContext(deps=Deps())

        await plan.startup(ctx)
        assert order == ["a", "b"]

    @pytest.mark.asyncio
    async def test_shutdown_runs_in_reverse_order(self) -> None:
        order: list[str] = []

        async def shut_a(ctx):
            order.append("a")

        async def shut_b(ctx):
            order.append("b")

        step_a = LifecycleStep(name="a", shutdown=shut_a)
        step_b = LifecycleStep(name="b", shutdown=shut_b)
        plan = LifecyclePlan.from_steps(step_a, step_b)
        ctx = ExecutionContext(deps=Deps())

        await plan.shutdown(ctx)
        assert order == ["b", "a"]

    @pytest.mark.asyncio
    async def test_startup_failure_runs_shutdown_for_executed(self) -> None:
        order: list[str] = []

        async def start_a(ctx):
            order.append("start_a")

        async def start_b(ctx):
            order.append("start_b")
            raise ValueError("fail")

        async def shut_a(ctx):
            order.append("shut_a")

        step_a = LifecycleStep(name="a", startup=start_a, shutdown=shut_a)
        step_b = LifecycleStep(name="b", startup=start_b)
        plan = LifecyclePlan.from_steps(step_a, step_b)
        ctx = ExecutionContext(deps=Deps())

        with pytest.raises(ValueError, match="fail"):
            await plan.startup(ctx)

        assert order == ["start_a", "start_b", "shut_a"]
