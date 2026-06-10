"""Unit tests for forze.application.execution.runtime."""

import pytest

from forze.application.execution import Deps, DepsRegistry
from forze.application.execution.lifecycle import LifecyclePlan, LifecycleStep
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException

# ----------------------- #


class TestExecutionRuntime:
    """Tests for ExecutionRuntime."""

    def test_create_context_builds_from_deps_registry(self) -> None:
        registration = Deps.plain({})
        rt = ExecutionRuntime(deps=DepsRegistry.from_deps(registration).freeze())
        rt.create_context()
        ctx = rt.get_context()
        assert ctx is not None
        assert ctx.deps.store.plain_deps == registration.store.plain_deps
        assert ctx.deps.store.routed_deps == registration.store.routed_deps

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
        plan = LifecyclePlan.from_steps(step).freeze()
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

        rt = ExecutionRuntime(lifecycle=LifecyclePlan.from_modules(_Module()).freeze())

        async with rt.scope():
            assert order == ["start"]

    @pytest.mark.asyncio
    async def test_scope_failed_startup_shuts_started_steps_down_exactly_once(
        self,
    ) -> None:
        startups: dict[str, int] = {}
        shutdowns: dict[str, int] = {}

        def _step(i: int, *, fail: bool = False) -> LifecycleStep:
            name = f"s{i}"

            async def up(_ctx) -> None:
                startups[name] = startups.get(name, 0) + 1
                if fail:
                    raise RuntimeError("startup failed at s3")

            async def down(_ctx) -> None:
                shutdowns[name] = shutdowns.get(name, 0) + 1

            return LifecycleStep(
                id=name,
                startup=up,
                shutdown=down,
                depends_on=(f"s{i - 1}",) if i > 1 else (),
            )

        plan = LifecyclePlan.from_steps(
            _step(1),
            _step(2),
            _step(3, fail=True),
            _step(4),
            _step(5),
        ).freeze()
        rt = ExecutionRuntime(lifecycle=plan)

        with pytest.raises(RuntimeError, match="startup failed at s3"):
            async with rt.scope():
                pass  # pragma: no cover - never reached

        # Steps 1-2 started and were shut down exactly once (by the rollback);
        # the scope-exit shutdown did not run them a second time.
        assert startups == {"s1": 1, "s2": 1, "s3": 1}
        assert shutdowns == {"s1": 1, "s2": 1}

    @pytest.mark.asyncio
    async def test_scope_shuts_every_started_step_down_exactly_once(self) -> None:
        shutdowns: dict[str, int] = {}

        def _step(i: int) -> LifecycleStep:
            name = f"s{i}"

            async def down(_ctx) -> None:
                shutdowns[name] = shutdowns.get(name, 0) + 1

            return LifecycleStep(
                id=name,
                shutdown=down,
                depends_on=(f"s{i - 1}",) if i > 1 else (),
            )

        plan = LifecyclePlan.from_steps(_step(1), _step(2), _step(3)).freeze()
        rt = ExecutionRuntime(lifecycle=plan)

        async with rt.scope():
            pass

        assert shutdowns == {"s1": 1, "s2": 1, "s3": 1}

    @pytest.mark.asyncio
    async def test_scope_resets_context_on_exit(self) -> None:

        rt = ExecutionRuntime(deps=DepsRegistry().freeze())
        async with rt.scope():
            ctx = rt.get_context()
            assert ctx is not None

        with pytest.raises(CoreException, match="not set"):
            rt.get_context()
