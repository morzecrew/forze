"""Tests for forze.application.execution.lifecycle."""

import asyncio

import pytest

from forze.application.contracts.execution import LifecycleStep, noop_lifecycle_hook
from forze.application.execution.context import ExecutionContext
from forze.application.execution.lifecycle import (
    FrozenLifecyclePlan,
    LifecyclePlan,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze_mock import MockDepsModule, MockState

# ----------------------- #


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule(state=MockState())())


def _wave_step_ids(frozen: FrozenLifecyclePlan) -> list[str]:
    return [step_id for wave in frozen.graph.waves for step_id in wave]


# ....................... #


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


class TestLifecyclePlanGraphFreeze:
    def test_builds_waves_from_capabilities(self) -> None:
        pool = LifecycleStep(id="pool", provides=("postgres.client",))
        warmup = LifecycleStep(id="warmup", requires=("postgres.client",))
        frozen = LifecyclePlan.from_steps(warmup, pool).freeze()

        assert frozen.graph.waves == (("pool",), ("warmup",))


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

        frozen = LifecyclePlan.from_steps(
            LifecycleStep(id="s", startup=up, shutdown=down),
        ).freeze()
        await frozen.startup(ctx)
        await frozen.shutdown(ctx)
        assert order == ["up", "down"]

    def test_with_steps_appends(self) -> None:
        plan = LifecyclePlan.from_steps(LifecycleStep(id="a"))
        extended = plan.with_steps(LifecycleStep(id="b"))

        assert tuple(s.id for s in extended.steps) == ("a", "b")

    def test_with_concurrent_sets_flag(self) -> None:
        plan = LifecyclePlan.from_steps(LifecycleStep(id="a")).with_concurrent()
        frozen = plan.freeze()

        assert frozen.concurrent is True

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

        frozen = LifecyclePlan.from_steps(
            LifecycleStep(id="first", startup=up_ok, shutdown=down),
            LifecycleStep(
                id="second",
                startup=up_fail,
                shutdown=down,
                depends_on=("first",),
            ),
        ).freeze()

        with pytest.raises(RuntimeError, match="startup failed"):
            await frozen.startup(ctx)

        assert order == ["up1", "down1"]

    @pytest.mark.asyncio
    async def test_startup_failure_concurrent_partial_wave_rollback(
        self, ctx: ExecutionContext
    ) -> None:
        order: list[str] = []

        async def up_slow(_ctx: ExecutionContext) -> None:
            order.append("up_slow")

        async def up_fail(_ctx: ExecutionContext) -> None:
            order.append("up_fail")
            raise RuntimeError("startup failed")

        async def down_slow(_ctx: ExecutionContext) -> None:
            order.append("down_slow")

        frozen = (
            LifecyclePlan.from_steps(
                LifecycleStep(id="slow", startup=up_slow, shutdown=down_slow),
                LifecycleStep(id="fail", startup=up_fail),
            )
            .with_concurrent()
            .freeze()
        )

        with pytest.raises(RuntimeError, match="startup failed"):
            await frozen.startup(ctx)

        assert "up_slow" in order
        assert "up_fail" in order
        assert order.count("down_slow") == 1

    @pytest.mark.asyncio
    async def test_shutdown_swallows_step_errors(self, ctx: ExecutionContext) -> None:
        async def down_fail(_ctx: ExecutionContext) -> None:
            raise RuntimeError("shutdown failed")

        frozen = LifecyclePlan.from_steps(
            LifecycleStep(id="bad", shutdown=down_fail),
            LifecycleStep(id="ok"),
        ).freeze()

        await frozen.startup(ctx)
        await frozen.shutdown(ctx)

    @pytest.mark.asyncio
    async def test_shutdown_after_failed_startup_rollback_is_noop(
        self, ctx: ExecutionContext
    ) -> None:
        shutdowns: list[str] = []

        async def up_fail(_ctx: ExecutionContext) -> None:
            raise RuntimeError("startup failed")

        def _down(name: str):
            async def _hook(_ctx: ExecutionContext) -> None:
                shutdowns.append(name)

            return _hook

        frozen = LifecyclePlan.from_steps(
            LifecycleStep(id="first", shutdown=_down("first")),
            LifecycleStep(
                id="second",
                startup=up_fail,
                shutdown=_down("second"),
                depends_on=("first",),
            ),
        ).freeze()

        with pytest.raises(RuntimeError, match="startup failed"):
            await frozen.startup(ctx)

        # Rollback shut "first" down once; "second" never started.
        assert shutdowns == ["first"]

        # A later full shutdown (e.g. runtime scope exit) must not re-run it.
        await frozen.shutdown(ctx)
        assert shutdowns == ["first"]

    @pytest.mark.asyncio
    async def test_second_shutdown_is_noop(self, ctx: ExecutionContext) -> None:
        shutdowns: list[str] = []

        async def down(_ctx: ExecutionContext) -> None:
            shutdowns.append("down")

        frozen = LifecyclePlan.from_steps(
            LifecycleStep(id="s", shutdown=down),
        ).freeze()

        await frozen.startup(ctx)
        await frozen.shutdown(ctx)
        await frozen.shutdown(ctx)

        assert shutdowns == ["down"]

    @pytest.mark.asyncio
    async def test_shutdown_without_startup_is_noop(
        self, ctx: ExecutionContext
    ) -> None:
        shutdowns: list[str] = []

        async def down(_ctx: ExecutionContext) -> None:
            shutdowns.append("down")

        frozen = LifecyclePlan.from_steps(
            LifecycleStep(id="s", shutdown=down),
        ).freeze()

        await frozen.shutdown(ctx)
        assert shutdowns == []

    @pytest.mark.asyncio
    async def test_concurrent_startup_runs_same_wave_in_parallel(
        self, ctx: ExecutionContext
    ) -> None:
        entered: list[str] = []
        release = asyncio.Event()

        async def up_a(_ctx: ExecutionContext) -> None:
            entered.append("a")
            await release.wait()

        async def up_b(_ctx: ExecutionContext) -> None:
            entered.append("b")
            await release.wait()

        frozen = (
            LifecyclePlan.from_steps(
                LifecycleStep(id="a", startup=up_a),
                LifecycleStep(id="b", startup=up_b),
            )
            .with_concurrent()
            .freeze()
        )

        startup_task = asyncio.create_task(frozen.startup(ctx))
        await asyncio.sleep(0.05)
        assert set(entered) == {"a", "b"}
        release.set()
        await startup_task

    @pytest.mark.asyncio
    async def test_sequential_startup_within_wave(self, ctx: ExecutionContext) -> None:
        entered: list[str] = []
        release = asyncio.Event()

        async def up_a(_ctx: ExecutionContext) -> None:
            entered.append("a")
            await release.wait()

        async def up_b(_ctx: ExecutionContext) -> None:
            entered.append("b")

        frozen = LifecyclePlan.from_steps(
            LifecycleStep(id="a", startup=up_a, priority=10),
            LifecycleStep(id="b", startup=up_b, priority=0),
        ).freeze()

        startup_task = asyncio.create_task(frozen.startup(ctx))
        await asyncio.sleep(0.05)
        assert entered == ["a"]
        release.set()
        await startup_task
        assert entered == ["a", "b"]

    def test_from_modules_freeze_resolves_order(self) -> None:
        pool = LifecycleStep(id="pool", provides=("db",))
        warmup = LifecycleStep(id="warmup", requires=("db",))

        class _Module:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (warmup, pool)

        frozen = LifecyclePlan.from_modules(_Module()).freeze()

        assert _wave_step_ids(frozen) == ["pool", "warmup"]
        assert frozen.graph.waves == (("pool",), ("warmup",))

    def test_freeze_merges_modules_and_steps(self) -> None:
        extra = LifecycleStep(id="extra")

        class _Module:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (LifecycleStep(id="from_module"),)

        frozen = LifecyclePlan.from_modules(_Module()).with_steps(extra).freeze()

        assert _wave_step_ids(frozen) == ["from_module", "extra"]

    def test_with_modules_appends(self) -> None:
        class _A:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (LifecycleStep(id="a"),)

        class _B:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (LifecycleStep(id="b"),)

        plan = LifecyclePlan.from_modules(_A()).with_modules(_B())

        assert len(plan.modules) == 2
