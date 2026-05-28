"""Tests for forze.application.execution.lifecycle."""

import pytest

from forze.application.contracts.execution import noop_lifecycle_hook
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle import LifecyclePlan, LifecycleStep
from forze_mock import MockDepsModule, MockState

# ----------------------- #


@pytest.fixture
def ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())


class TestNoopLifecycleHook:
    @pytest.mark.asyncio
    async def test_noop_lifecycle_hook_returns_none(
        self, ctx: ExecutionContext
    ) -> None:
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
    async def test_startup_and_shutdown_run_in_order(
        self, ctx: ExecutionContext
    ) -> None:
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

    def test_with_steps_appends(self) -> None:
        plan = LifecyclePlan.from_steps(LifecycleStep(id="a"))
        extended = plan.with_steps(LifecycleStep(id="b"))

        assert tuple(s.id for s in extended.steps) == ("a", "b")

    @pytest.mark.asyncio
    async def test_startup_failure_runs_shutdown_in_reverse(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []

        async def up_ok(_ctx: ExecutionContext) -> None:
            order.append("up1")

        async def up_fail(_ctx: ExecutionContext) -> None:
            raise RuntimeError("startup failed")

        async def down(_ctx: ExecutionContext) -> None:
            order.append("down1")

        plan = LifecyclePlan.from_steps(
            LifecycleStep(id="first", startup=up_ok, shutdown=down),
            LifecycleStep(id="second", startup=up_fail, shutdown=down),
        )

        with pytest.raises(RuntimeError, match="startup failed"):
            await plan.startup(ctx)

        assert order == ["up1", "down1"]

    @pytest.mark.asyncio
    async def test_shutdown_swallows_step_errors(self, ctx: ExecutionContext) -> None:
        async def down_fail(_ctx: ExecutionContext) -> None:
            raise RuntimeError("shutdown failed")

        plan = LifecyclePlan.from_steps(
            LifecycleStep(id="bad", shutdown=down_fail),
            LifecycleStep(id="ok"),
        )

        await plan.shutdown(ctx)

    def test_from_modules_build_resolves_order(self) -> None:
        pool = LifecycleStep(id="pool", provides=("db",))
        warmup = LifecycleStep(id="warmup", requires=("db",))

        class _Module:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (warmup, pool)

        built = LifecyclePlan.from_modules(_Module()).build()

        assert tuple(s.id for s in built.steps) == ("pool", "warmup")
        assert built.modules == ()

    def test_build_merges_modules_and_steps(self) -> None:
        extra = LifecycleStep(id="extra")

        class _Module:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (LifecycleStep(id="from_module"),)

        built = LifecyclePlan.from_modules(_Module()).with_steps(extra).build()

        assert tuple(s.id for s in built.steps) == ("from_module", "extra")

    def test_with_modules_appends(self) -> None:
        class _A:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (LifecycleStep(id="a"),)

        class _B:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (LifecycleStep(id="b"),)

        plan = LifecyclePlan.from_modules(_A()).with_modules(_B())

        assert len(plan.modules) == 2
