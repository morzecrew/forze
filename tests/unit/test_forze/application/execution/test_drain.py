"""Drain semantics: in-flight accounting, rejection while draining, bounded shutdown wait."""

from __future__ import annotations

import asyncio
from typing import Any

import attrs
import pytest

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext, build_runtime
from forze.application.execution.context import OperationDrainGate
from forze.application.execution.lifecycle import LifecyclePlan, LifecycleStep
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_deps

# ----------------------- #


@pytest.fixture
def ctx() -> ExecutionContext:
    return context_from_deps(MockDepsModule()())


@attrs.define(slots=True, kw_only=True, frozen=True)
class EchoHandler(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return f"handler:{args}"


def _echo_registry() -> Any:
    return OperationRegistry(handlers={"op": lambda _ctx: EchoHandler()}).freeze()


# ----------------------- #


class TestOperationDrainGate:
    def test_admit_and_release_count(self) -> None:
        gate = OperationDrainGate()

        gate.admit("op")
        gate.admit("op")
        assert gate.in_flight == 2

        gate.release()
        gate.release()
        assert gate.in_flight == 0
        assert gate.draining is False

    async def test_admit_while_draining_raises_throttled(self) -> None:
        gate = OperationDrainGate()

        assert await gate.drain(0.0) is True

        with pytest.raises(CoreException) as ei:
            gate.admit("op")

        assert ei.value.kind is ExceptionKind.THROTTLED
        assert ei.value.code == "draining"

    async def test_drain_idle_returns_immediately(self) -> None:
        gate = OperationDrainGate()

        assert await gate.drain(0.0) is True
        assert gate.draining is True

    async def test_drain_waits_for_in_flight(self) -> None:
        gate = OperationDrainGate()
        gate.admit("op")

        drain_task = asyncio.create_task(gate.drain(5.0))
        await asyncio.sleep(0)
        assert not drain_task.done()

        gate.release()

        assert await drain_task is True
        assert gate.in_flight == 0

    async def test_drain_timeout_expires_with_count_preserved(self) -> None:
        gate = OperationDrainGate()
        gate.admit("op")

        assert await gate.drain(0.01) is False
        assert gate.draining is True
        assert gate.in_flight == 1

        gate.release()


class TestEngineGateIntegration:
    @pytest.mark.asyncio
    async def test_invocation_counts_in_flight(self, ctx: ExecutionContext) -> None:
        seen: list[int] = []

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class PeekHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                seen.append(ctx.drain_gate.in_flight)
                return args

        reg = OperationRegistry(handlers={"op": lambda _ctx: PeekHandler()}).freeze()

        await reg.resolve("op", ctx)("x")

        assert seen == [1]
        assert ctx.drain_gate.in_flight == 0

    @pytest.mark.asyncio
    async def test_draining_rejects_new_top_level_invocation(
        self, ctx: ExecutionContext
    ) -> None:
        resolved = _echo_registry().resolve("op", ctx)

        await ctx.drain_gate.drain(0.0)

        with pytest.raises(CoreException) as ei:
            await resolved("x")

        assert ei.value.kind is ExceptionKind.THROTTLED
        assert ei.value.code == "draining"

    @pytest.mark.asyncio
    async def test_release_runs_on_failure(self, ctx: ExecutionContext) -> None:
        @attrs.define(slots=True, kw_only=True, frozen=True)
        class FailHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                raise RuntimeError("boom")

        reg = OperationRegistry(handlers={"op": lambda _ctx: FailHandler()}).freeze()

        with pytest.raises(RuntimeError):
            await reg.resolve("op", ctx)("x")

        assert ctx.drain_gate.in_flight == 0

    @pytest.mark.asyncio
    async def test_nested_dispatch_bypasses_drain_gate(
        self, ctx: ExecutionContext
    ) -> None:
        """An admitted operation's nested invocations ride its slot while draining."""

        started = asyncio.Event()
        release = asyncio.Event()
        inner = _echo_registry().resolve("op", ctx)

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class OuterHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                started.set()
                await release.wait()
                # The scope is draining by now; this nested call must still run.
                return await inner(args)

        reg = OperationRegistry(
            handlers={"outer": lambda _ctx: OuterHandler()}
        ).freeze()
        outer_task = asyncio.create_task(reg.resolve("outer", ctx)("x"))

        await started.wait()
        drain_task = asyncio.create_task(ctx.drain_gate.drain(5.0))
        await asyncio.sleep(0)
        release.set()

        assert await outer_task == "handler:x"
        assert await drain_task is True


class TestRuntimeDrain:
    @pytest.mark.asyncio
    async def test_scope_exit_waits_for_in_flight_before_lifecycle_shutdown(
        self,
    ) -> None:
        order: list[str] = []
        started = asyncio.Event()
        release = asyncio.Event()

        async def _shut(_ctx: Any) -> None:
            order.append("lifecycle_shutdown")

        plan = LifecyclePlan.from_steps(
            LifecycleStep(id="s", shutdown=_shut)
        ).freeze()
        rt = ExecutionRuntime(lifecycle=plan, drain_timeout=5.0)

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class SlowHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                started.set()
                await release.wait()
                order.append("handler_done")
                return args

        reg = OperationRegistry(handlers={"op": lambda _ctx: SlowHandler()}).freeze()

        async with rt.scope():
            ctx = rt.get_context()
            task = asyncio.create_task(reg.resolve("op", ctx)("x"))
            await started.wait()

            async def _release_once_draining() -> None:
                while not ctx.drain_gate.draining:
                    await asyncio.sleep(0)
                release.set()

            releaser = asyncio.create_task(_release_once_draining())

        # The handler finished inside the drain window, before lifecycle teardown.
        assert order == ["handler_done", "lifecycle_shutdown"]
        assert await task == "x"
        await releaser

    @pytest.mark.asyncio
    async def test_scope_exit_proceeds_after_drain_timeout(self) -> None:
        rt = ExecutionRuntime(drain_timeout=0.01)
        stall = asyncio.Event()

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class StuckHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                await stall.wait()
                return args

        reg = OperationRegistry(handlers={"op": lambda _ctx: StuckHandler()}).freeze()

        async with rt.scope():
            ctx = rt.get_context()
            task = asyncio.create_task(reg.resolve("op", ctx)("x"))
            await asyncio.sleep(0)

        # Scope exited despite the stuck operation; clean it up.
        assert not task.done()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    def test_build_runtime_passes_drain_timeout(self) -> None:
        assert build_runtime(drain_timeout=3.0).drain_timeout == 3.0
