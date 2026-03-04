"""Unit tests for forze.application.execution.runtime."""

import pytest

from forze.application.execution import Deps, DepsPlan, ExecutionContext
from forze.application.execution.lifecycle import LifecyclePlan, LifecycleStep
from forze.application.execution.runtime import ExecutionRuntime

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
        from forze.base.errors import CoreError

        rt = ExecutionRuntime()
        with pytest.raises(CoreError, match="not set"):
            rt.get_context()

    @pytest.mark.asyncio
    async def test_scope_runs_startup_and_shutdown(self) -> None:
        order: list[str] = []

        async def start(ctx):
            order.append("start")

        async def shut(ctx):
            order.append("shut")

        step = LifecycleStep(name="s", startup=start, shutdown=shut)
        plan = LifecyclePlan.from_steps(step)
        rt = ExecutionRuntime(lifecycle=plan)

        async with rt.scope():
            assert rt.get_context() is not None
            assert order == ["start"]

        assert order == ["start", "shut"]

    @pytest.mark.asyncio
    async def test_scope_resets_context_on_exit(self) -> None:
        from forze.base.errors import CoreError

        rt = ExecutionRuntime(deps=DepsPlan.from_modules(lambda: Deps()))
        async with rt.scope():
            ctx = rt.get_context()
            assert ctx is not None

        with pytest.raises(CoreError, match="not set"):
            rt.get_context()
