"""Drain semantics: in-flight accounting, rejection while draining, bounded shutdown wait."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import attrs
import pytest

from forze.application.contracts.execution import (
    Handler,
    LifecycleStep,
    OnSuccessStep,
    TwoPhaseHandler,
)
from forze.application.contracts.resilience import (
    HedgeStrategy,
    ResiliencePolicy,
    TimeoutStrategy,
)
from forze.application.execution import ExecutionContext, build_runtime
from forze.application.execution.context import OperationDrainGate
from forze.application.execution.graph_run import run_wave_forward
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.resilience import InProcessResilienceExecutor
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

    async def test_cancel_in_flight_is_a_noop_when_idle(self) -> None:
        gate = OperationDrainGate()

        assert await gate.cancel_in_flight(grace=1.0) == 0

    async def test_cancel_in_flight_cancels_tracked_operation_tasks(self) -> None:
        gate = OperationDrainGate()
        started = asyncio.Event()

        async def _op() -> None:
            gate.admit("op")

            try:
                started.set()
                await asyncio.sleep(3600)  # stuck; only cancellation ends it

            finally:
                gate.release()

        task = asyncio.create_task(_op())
        await started.wait()

        assert await gate.drain(0.01) is False  # times out with the op in flight

        cancelled = await gate.cancel_in_flight(grace=1.0)

        assert cancelled == 1
        assert task.cancelled()  # the abandoned op was cancelled and unwound
        assert gate.in_flight == 0  # its release ran


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

    @pytest.mark.asyncio
    async def test_spawned_operation_is_counted_and_cancellable(
        self, ctx: ExecutionContext
    ) -> None:
        """An operation a handler spawns via ``create_task`` is a *detached*
        top-level driver on a new task — it must be admitted (counted in flight)
        and its task tracked, so drain awaits it or ``cancel_in_flight`` ends it.

        It inherits the enclosing active-operation marker through the copied
        context, so a presence-only nesting check would misclassify it as nested
        (``gate=None``), leaving it uncounted and un-cancellable — it would escape
        drain and run on against the clients teardown is closing.
        """

        release = asyncio.Event()
        child_started = asyncio.Event()
        spawned: list[asyncio.Task[Any]] = []

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class ChildHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                child_started.set()
                await release.wait()  # stays in flight until released/cancelled
                return args

        child = OperationRegistry(
            handlers={"child": lambda _ctx: ChildHandler()}
        ).freeze().resolve("child", ctx)

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class OuterHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                # Fire-and-forget: spawn the child and return WITHOUT awaiting it.
                spawned.append(asyncio.create_task(child(args)))
                return args

        outer = OperationRegistry(
            handlers={"outer": lambda _ctx: OuterHandler()}
        ).freeze().resolve("outer", ctx)

        assert await outer("x") == "x"
        # Outer released its own slot on return; the child runs on detached.
        await child_started.wait()

        assert ctx.drain_gate.in_flight == 1  # child was admitted, not skipped

        # The child outlives its spawner: drain can't complete, and the gate holds
        # its task so shutdown can cancel it before teardown closes its clients.
        assert await ctx.drain_gate.drain(0.01) is False
        assert ctx.drain_gate.in_flight == 1

        cancelled = await ctx.drain_gate.cancel_in_flight(grace=1.0)

        assert cancelled == 1
        assert spawned[0].cancelled()  # the escaped op was reachable and cancelled
        assert ctx.drain_gate.in_flight == 0  # its release ran on unwind


class TestEngineContinuationsRideAdmittedSlot:
    """Engine-internal task hops stay inside the admitted operation's slot.

    The engine's own machinery routinely re-enters dispatch from a task other
    than the one the operation was admitted on — a two-phase ``prepare`` task, a
    hedged attempt, the post-commit callback runner, a concurrent graph wave.
    Each of those is a *continuation* of the already-admitted operation: it must
    not be re-admitted (and so must not be rejected with THROTTLED ``draining``
    mid-shutdown), and drain must keep waiting for the whole chain because the
    admitted slot is held until the chain is awaited.

    Contrast with ``test_spawned_operation_is_counted_and_cancellable`` /
    ``test_handler_spawned_sibling_is_throttled_while_draining``: a task *user
    code* spawns is a detached top-level driver and stays fully gated.
    """

    @pytest.mark.asyncio
    async def test_after_commit_dispatch_completes_while_draining(
        self, ctx: ExecutionContext
    ) -> None:
        """A plan's after-commit dispatch runs on the post-commit runner task;
        while draining it must still complete as part of the admitted op (its
        failure would otherwise be swallowed as a non-fatal after-commit error,
        silently dropping the dispatch)."""

        started = asyncio.Event()
        release = asyncio.Event()
        after_commit: list[tuple[str, bool, int]] = []
        inner = _echo_registry().resolve("op", ctx)

        def _dispatch_factory(_ctx: Any) -> Any:
            async def _hook(_args: str, _result: str) -> None:
                dispatched = await inner("ac")
                after_commit.append(
                    (dispatched, ctx.drain_gate.draining, ctx.drain_gate.in_flight)
                )

            return _hook

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class WaitHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                started.set()
                await release.wait()
                return args

        reg = (
            OperationRegistry(handlers={"outer": lambda _c: WaitHandler()})
            .bind("outer")
            .bind_tx()
            .set_route("mock")
            .after_commit(OnSuccessStep(id="ac", factory=_dispatch_factory))
            .finish(deep=True)
            .freeze()
        )

        outer_task = asyncio.create_task(reg.resolve("outer", ctx)("x"))
        await started.wait()

        drain_task = asyncio.create_task(ctx.drain_gate.drain(5.0))
        await asyncio.sleep(0)
        assert not drain_task.done()  # the admitted op holds its slot

        release.set()

        assert await outer_task == "x"
        # The dispatch ran while draining, rode the admitted slot (in-flight
        # stayed at the outer op's single admission), and was not throttled.
        assert after_commit == [("handler:ac", True, 1)]
        assert await drain_task is True

    @pytest.mark.asyncio
    async def test_two_phase_prepare_dispatch_completes_while_draining(
        self, ctx: ExecutionContext
    ) -> None:
        """``prepare`` runs on its own engine task; a dispatch it makes while
        draining is a continuation of the admitted op, not a fresh admission."""

        started = asyncio.Event()
        release = asyncio.Event()
        seen_in_flight: list[int] = []
        inner = _echo_registry().resolve("op", ctx)

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class DispatchingTwoPhase(TwoPhaseHandler[str, str, str]):
            async def prepare(self, args: str) -> str:
                started.set()
                await release.wait()
                seen_in_flight.append(ctx.drain_gate.in_flight)
                return await inner(args)  # must not be THROTTLED "draining"

            async def apply(self, args: str, payload: str) -> str:
                return f"applied:{payload}"

        reg = (
            OperationRegistry(handlers={"outer": lambda _c: DispatchingTwoPhase()})
            .bind("outer")
            .two_phase()
            .bind_tx()
            .set_route("mock")
            .finish(deep=True)
            .freeze()
        )

        outer_task = asyncio.create_task(reg.resolve("outer", ctx)("x"))
        await started.wait()

        drain_task = asyncio.create_task(ctx.drain_gate.drain(5.0))
        await asyncio.sleep(0)
        assert not drain_task.done()

        release.set()

        assert await outer_task == "applied:handler:x"
        assert seen_in_flight == [1]  # prepare rode the outer admission
        assert await drain_task is True

    @pytest.mark.asyncio
    async def test_hedged_attempt_dispatch_completes_while_draining(
        self, ctx: ExecutionContext
    ) -> None:
        """A hedged attempt runs on its own engine task; a dispatch inside the
        attempt is a continuation of the admitted op."""

        started = asyncio.Event()
        release = asyncio.Event()
        inner = _echo_registry().resolve("op", ctx)
        executor = InProcessResilienceExecutor(
            policies={
                "h": ResiliencePolicy(
                    name="h",
                    strategies=(TimeoutStrategy(timeout=timedelta(seconds=10)),),
                    hedge=HedgeStrategy(
                        delay=timedelta(seconds=10), max_attempts=2
                    ),
                )
            }
        )

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class HedgingHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                started.set()
                await release.wait()
                return await executor.run_hedged(
                    lambda: inner(args), policy="h", route="r"
                )

        reg = OperationRegistry(
            handlers={"outer": lambda _c: HedgingHandler()}
        ).freeze()
        outer_task = asyncio.create_task(reg.resolve("outer", ctx)("x"))
        await started.wait()

        drain_task = asyncio.create_task(ctx.drain_gate.drain(5.0))
        await asyncio.sleep(0)
        assert not drain_task.done()

        release.set()

        assert await outer_task == "handler:x"
        assert await drain_task is True

    @pytest.mark.asyncio
    async def test_concurrent_wave_dispatch_completes_while_draining(
        self, ctx: ExecutionContext
    ) -> None:
        """The engine's concurrent graph-wave fan-out runs each step on a
        gather-spawned task; a dispatch inside a step is a continuation of the
        admitted op."""

        started = asyncio.Event()
        release = asyncio.Event()
        results: list[str] = []
        inner = _echo_registry().resolve("op", ctx)

        async def _dispatch_step(suffix: str) -> None:
            results.append(await inner(suffix))

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class FanOutHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                started.set()
                await release.wait()
                await run_wave_forward(
                    ("a", "b"),
                    {"a": "a", "b": "b"},
                    _dispatch_step,
                    concurrent=True,
                )
                return args

        reg = OperationRegistry(
            handlers={"outer": lambda _c: FanOutHandler()}
        ).freeze()
        outer_task = asyncio.create_task(reg.resolve("outer", ctx)("x"))
        await started.wait()

        drain_task = asyncio.create_task(ctx.drain_gate.drain(5.0))
        await asyncio.sleep(0)
        assert not drain_task.done()

        release.set()

        assert await outer_task == "x"
        assert sorted(results) == ["handler:a", "handler:b"]
        assert await drain_task is True

    @pytest.mark.asyncio
    async def test_handler_spawned_sibling_is_throttled_while_draining(
        self, ctx: ExecutionContext
    ) -> None:
        """Regression: a task *user code* spawns is not an engine continuation —
        it stays a fresh top-level driver and is rejected while draining."""

        started = asyncio.Event()
        release = asyncio.Event()
        outcomes: list[tuple[ExceptionKind, str | None]] = []
        inner = _echo_registry().resolve("op", ctx)

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class SpawningHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                started.set()
                await release.wait()
                sibling = asyncio.create_task(inner(args))

                try:
                    await sibling

                except CoreException as error:
                    outcomes.append((error.kind, error.code))

                return args

        reg = OperationRegistry(
            handlers={"outer": lambda _c: SpawningHandler()}
        ).freeze()
        outer_task = asyncio.create_task(reg.resolve("outer", ctx)("x"))
        await started.wait()

        drain_task = asyncio.create_task(ctx.drain_gate.drain(5.0))
        await asyncio.sleep(0)
        release.set()

        assert await outer_task == "x"
        assert outcomes == [(ExceptionKind.THROTTLED, "draining")]
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

        plan = LifecyclePlan.from_steps(LifecycleStep(id="s", shutdown=_shut)).freeze()
        rt = ExecutionRuntime(
            lifecycle=plan,
            drain_timeout=timedelta(seconds=5.0),
        )

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
    async def test_scope_exit_cancels_abandoned_ops_after_drain_timeout(self) -> None:
        rt = ExecutionRuntime(drain_timeout=timedelta(seconds=0.01))
        stall = asyncio.Event()
        saw_cancel = asyncio.Event()

        @attrs.define(slots=True, kw_only=True, frozen=True)
        class StuckHandler(Handler[str, str]):
            async def __call__(self, args: str) -> str:
                try:
                    await stall.wait()  # never set — only cancellation ends it

                except asyncio.CancelledError:
                    saw_cancel.set()
                    raise

                return args

        reg = OperationRegistry(handlers={"op": lambda _ctx: StuckHandler()}).freeze()

        async with rt.scope():
            ctx = rt.get_context()
            task = asyncio.create_task(reg.resolve("op", ctx)("x"))
            await asyncio.sleep(0)

        # The drain window expired, so the runtime cancelled the abandoned operation and
        # let it unwind *before* lifecycle teardown — no manual cleanup, no orphan task
        # running against closing clients.
        assert task.done()
        assert task.cancelled()
        assert saw_cancel.is_set()

    @pytest.mark.asyncio
    async def test_scope_exit_closes_background_owners(self) -> None:
        rt = ExecutionRuntime()

        class _Owner:
            def __init__(self) -> None:
                self.closed = False

            async def aclose(self) -> None:
                self.closed = True

        owner = _Owner()

        async with rt.scope():
            rt.get_context().background_owners.register(owner)

        # Shutdown cancelled/closed the registered owner (before lifecycle teardown) so its
        # detached work never runs on against a closing client.
        assert owner.closed

    def test_build_runtime_passes_drain_timeout(self) -> None:
        assert build_runtime(
            drain_timeout=timedelta(seconds=3.0)
        ).drain_timeout == timedelta(seconds=3.0)
