"""Unit tests for forze.application.execution.runtime."""

import pytest

from forze.application.execution import Deps, DepsPlan
from forze.application.execution.lifecycle import LifecyclePlan, LifecycleStep
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException

# ----------------------- #


class TestExecutionRuntime:
    """Tests for ExecutionRuntime."""

    def test_create_context_builds_from_deps_plan(self) -> None:
        deps = Deps()
        plan = DepsPlan.from_modules(lambda: deps)
        rt = ExecutionRuntime(deps=plan)
        rt.create_context()
        ctx = rt.get_context()
        assert ctx is not None
        assert ctx.deps == deps

    def test_get_context_outside_scope_raises(self) -> None:

        rt = ExecutionRuntime()
        with pytest.raises(CoreException, match="not set"):
            rt.get_context()

    @pytest.mark.asyncio
    async def test_scope_runs_startup_and_shutdown(self) -> None:
        order: list[str] = []

        async def start(ctx):
            order.append("start")

        async def shut(ctx):
            order.append("shut")

        step = LifecycleStep(id="s", startup=start, shutdown=shut)
        plan = LifecyclePlan.from_steps(step)
        rt = ExecutionRuntime(lifecycle=plan)

        async with rt.scope():
            assert rt.get_context() is not None
            assert order == ["start"]

        assert order == ["start", "shut"]

    @pytest.mark.asyncio
    async def test_scope_builds_lifecycle_from_modules(self) -> None:
        order: list[str] = []

        async def start(_ctx) -> None:
            order.append("start")

        class _Module:
            def __call__(self) -> tuple[LifecycleStep, ...]:
                return (LifecycleStep(id="s", startup=start),)

        rt = ExecutionRuntime(lifecycle=LifecyclePlan.from_modules(_Module()))

        async with rt.scope():
            assert order == ["start"]

    @pytest.mark.asyncio
    async def test_scope_resets_context_on_exit(self) -> None:

        rt = ExecutionRuntime(deps=DepsPlan.from_modules(Deps))
        async with rt.scope():
            ctx = rt.get_context()
            assert ctx is not None

        with pytest.raises(CoreException, match="not set"):
            rt.get_context()
