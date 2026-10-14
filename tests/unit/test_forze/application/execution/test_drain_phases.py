"""Announcing a drain, and the phases that run between the announcement and teardown."""

from __future__ import annotations

import asyncio
import contextvars
import uuid
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import attrs
import pytest

from forze.application.contracts.execution import Handler, LifecycleStep
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext, build_runtime
from forze.application.execution.lifecycle.run import DRAIN_PHASE_CANCEL_GRACE_SECONDS
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import cpu_executor_bound

# ----------------------- #

Phase = Callable[[timedelta], Awaitable[None]]


def _registering(*phases: tuple[str, Phase, dict[str, Any]]) -> LifecycleStep:
    """A step that registers *phases* at startup, the way a real step would."""

    async def startup(ctx: ExecutionContext) -> None:
        for name, phase, options in phases:
            ctx.drain_phases.register(name, phase, **options)

    return LifecycleStep(id="phases", startup=startup)


def _runtime(*steps: LifecycleStep, budget: timedelta = timedelta(seconds=5)) -> ExecutionRuntime:
    return build_runtime(
        lifecycle_steps=steps,
        drain_timeout=timedelta(0),
        drain_budget=budget,
    )


@attrs.define(slots=True, kw_only=True, frozen=True)
class _Echo(Handler[str, str]):
    async def __call__(self, args: str) -> str:
        return args


# ----------------------- #


class TestBeginDraining:
    @pytest.mark.asyncio
    async def test_readiness_fails_while_operations_are_still_admitted(self) -> None:
        runtime = _runtime()
        echo = OperationRegistry(handlers={"op": lambda _ctx: _Echo()}).freeze()

        async with runtime.scope():
            ctx = runtime.get_context()

            runtime.begin_draining()
            runtime.begin_draining()  # idempotent

            assert runtime.ready is False
            assert runtime.draining is True
            assert ctx.lifecycle.announced is True
            # Readiness is withdrawn; operations are not refused.
            assert ctx.drain_gate.draining is False
            assert await echo.resolve("op", ctx)("still served") == "still served"

        # Shutdown afterwards still closed the gate.
        assert ctx.drain_gate.draining is True

    # ....................... #

    def test_outside_a_scope_it_does_nothing(self) -> None:
        runtime = _runtime()

        runtime.begin_draining()

        assert runtime.draining is False


# ----------------------- #


class TestDrainPhases:
    @pytest.mark.asyncio
    async def test_phases_run_once_in_order_each_told_its_allowance(self) -> None:
        ran: list[tuple[str, timedelta]] = []

        def recording(name: str) -> Phase:
            async def phase(allowed: timedelta) -> None:
                ran.append((name, allowed))

            return phase

        runtime = _runtime(
            _registering(
                ("late", recording("late"), {"order": 2}),
                ("capped", recording("capped"), {"order": 1, "max_duration": timedelta(seconds=1)}),
                ("uncapped", recording("uncapped"), {"order": 1}),
            ),
            budget=timedelta(seconds=5),
        )

        async with runtime.scope():
            await runtime.drain(timedelta(seconds=5))
            await runtime.drain(timedelta(seconds=5))  # at most once per scope

        assert [name for name, _ in ran] == ["capped", "uncapped", "late"]
        assert ran[0][1] == timedelta(seconds=1)
        assert timedelta(seconds=4.5) < ran[1][1] <= timedelta(seconds=5)

    # ....................... #

    @pytest.mark.asyncio
    async def test_an_overrunning_phase_is_cancelled_and_the_next_still_runs(self) -> None:
        cancelled = asyncio.Event()
        after: list[timedelta] = []

        async def overrun(_allowed: timedelta) -> None:
            try:
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def next_phase(allowed: timedelta) -> None:
            after.append(allowed)

        runtime = _runtime(
            _registering(
                ("overrun", overrun, {"order": 1, "max_duration": timedelta(seconds=0.2)}),
                ("next", next_phase, {"order": 2}),
            )
        )

        async with runtime.scope():
            clock = asyncio.get_running_loop()
            started = clock.time()

            await runtime.drain(timedelta(seconds=5))

            elapsed = clock.time() - started

        assert cancelled.is_set()
        assert 0.2 <= elapsed < 1.0
        assert len(after) == 1 and timedelta(seconds=4) < after[0] < timedelta(seconds=4.9)

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_phase_that_swallows_cancellation_is_abandoned_within_the_bound(
        self,
    ) -> None:
        stop = False
        swallowed = 0
        alive = 0

        async def stubborn(_allowed: timedelta) -> None:
            nonlocal swallowed, alive

            for _ in range(400):  # bounded: 20s at most, whatever happens to `stop`
                if stop:
                    return

                alive += 1

                try:
                    await asyncio.sleep(0.05)

                except asyncio.CancelledError:
                    swallowed += 1

        budget = 0.3
        runtime = _runtime(_registering(("stubborn", stubborn, {})))

        async with runtime.scope():
            clock = asyncio.get_running_loop()
            started = clock.time()

            await asyncio.wait_for(runtime.drain(timedelta(seconds=budget)), timeout=10.0)

            elapsed = clock.time() - started
            beats_at_return = alive

            await asyncio.sleep(0.2)

            # Abandoned, not stopped: it ignored the cancel and is still running.
            assert swallowed >= 1
            assert alive > beats_at_return

            stop = True

        assert budget + DRAIN_PHASE_CANCEL_GRACE_SECONDS - 0.1 <= elapsed
        assert elapsed < budget + DRAIN_PHASE_CANCEL_GRACE_SECONDS + 0.5

    # ....................... #

    @pytest.mark.asyncio
    async def test_phases_wait_out_the_announce_delay_with_readiness_failed(self) -> None:
        seen: list[tuple[float, bool]] = []

        async def phase(_allowed: timedelta) -> None:
            seen.append((asyncio.get_running_loop().time(), runtime.ready))

        runtime = _runtime(_registering(("phase", phase, {})))

        async with runtime.scope():
            clock = asyncio.get_running_loop()
            started = clock.time()
            drain = asyncio.create_task(
                runtime.drain(timedelta(seconds=1), announce_delay=timedelta(seconds=0.2))
            )
            await asyncio.sleep(0.05)

            # Announced at once, phases not yet started.
            assert runtime.ready is False
            assert seen == []

            await asyncio.wait_for(drain, timeout=5.0)

        assert len(seen) == 1
        assert seen[0][0] - started >= 0.2
        assert seen[0][1] is False

    # ....................... #

    @pytest.mark.asyncio
    async def test_cancelling_a_drain_cancels_the_running_phase(self) -> None:
        """What a second signal does: the phase is cancelled, and never runs again."""

        phase_cancelled = asyncio.Event()
        runs = 0

        async def slow(_allowed: timedelta) -> None:
            nonlocal runs
            runs += 1

            try:
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                phase_cancelled.set()
                raise

        runtime = _runtime(_registering(("slow", slow, {})))

        async with runtime.scope():
            drain = asyncio.create_task(runtime.drain(timedelta(seconds=30)))
            await asyncio.sleep(0.05)

            drain.cancel()

            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(drain, timeout=5.0)

            await asyncio.wait_for(phase_cancelled.wait(), timeout=5.0)

        # The scope's shutdown found the drain claimed, and did not run the phase again.
        assert runs == 1

    # ....................... #

    @pytest.mark.asyncio
    async def test_phases_run_in_the_scopes_context_not_the_callers(self) -> None:
        """A drain asked for by a request — or by a signal that interrupted one — must not
        hand the request's bindings to the phases, and must give them the scope's."""

        seen: list[tuple[TenantIdentity | None, bool]] = []

        async def phase(_allowed: timedelta) -> None:
            seen.append((runtime.get_context().inv_ctx.get_tenant(), cpu_executor_bound()))

        runtime = _runtime(_registering(("phase", phase, {})))

        async with runtime.scope():
            ctx = runtime.get_context()
            tenant = TenantIdentity(tenant_id=uuid.uuid4(), tenant_key="acme")

            async def request() -> None:
                assert cpu_executor_bound() is False  # none of the scope's bindings

                with ctx.inv_ctx.bind_identity(tenant=tenant):
                    await runtime.drain(timedelta(seconds=1))

            # A fresh context, as a server's request task has: a tenant, no scope bindings.
            await asyncio.get_running_loop().create_task(
                request(), context=contextvars.Context()
            )

        assert seen == [(None, True)]

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_phase_left_less_than_the_floor_is_skipped(self) -> None:
        ran: list[str] = []

        async def phase(_allowed: timedelta) -> None:
            ran.append("phase")

        runtime = _runtime(_registering(("starved", phase, {})))

        async with runtime.scope():
            await runtime.drain(timedelta(seconds=0.05))

        assert ran == []

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_failing_phase_does_not_stop_the_next(self) -> None:
        ran: list[str] = []

        async def boom(_allowed: timedelta) -> None:
            raise RuntimeError("phase failed")

        async def phase(_allowed: timedelta) -> None:
            ran.append("next")

        runtime = _runtime(
            _registering(("boom", boom, {"order": 1}), ("next", phase, {"order": 2}))
        )

        async with runtime.scope():
            await runtime.drain(timedelta(seconds=1))

        assert ran == ["next"]


# ----------------------- #


class TestShutdownDrainsFirst:
    @pytest.mark.asyncio
    async def test_shutdown_runs_the_phases_before_closing_the_gate_and_tearing_down(
        self,
    ) -> None:
        seen: list[tuple[str, bool, bool]] = []
        captured: list[ExecutionContext] = []

        async def startup(ctx: ExecutionContext) -> None:
            captured.append(ctx)

            async def phase(_allowed: timedelta) -> None:
                seen.append(("phase", ctx.lifecycle.announced, ctx.drain_gate.draining))

            ctx.drain_phases.register("phase", phase)

        async def teardown(ctx: ExecutionContext) -> None:
            seen.append(("teardown", ctx.lifecycle.announced, ctx.drain_gate.draining))

        runtime = _runtime(LifecycleStep(id="step", startup=startup, shutdown=teardown))

        async with runtime.scope():
            pass

        # Announced, gate still open during the phase; closed by the time teardown runs.
        assert seen == [("phase", True, False), ("teardown", True, True)]

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_drain_that_raises_does_not_stop_the_teardown(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        torn_down: list[bool] = []

        async def teardown(ctx: ExecutionContext) -> None:
            torn_down.append(ctx.drain_gate.draining)

        async def failing_drain(*_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError("drain failed")

        runtime = _runtime(LifecycleStep(id="step", shutdown=teardown))
        monkeypatch.setattr(ExecutionRuntime, "drain", failing_drain)

        async with runtime.scope():
            pass

        assert torn_down == [True]

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_second_cancel_during_the_phases_still_tears_down(self) -> None:
        """Ctrl+C under ``asyncio.run`` cancels the main task; a second Ctrl+C cancels it
        again, here while ``shutdown()`` runs a phase. Teardown must still run, and the
        cancellation must still come out the other end."""

        events: list[object] = []
        entered = asyncio.Event()
        phase_started = asyncio.Event()

        async def slow(_allowed: timedelta) -> None:
            events.append("phase-started")
            phase_started.set()

            try:
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                events.append("phase-cancelled")
                raise

        async def teardown(ctx: ExecutionContext) -> None:
            events.append(("teardown", ctx.drain_gate.draining))

        runtime = _runtime(
            _registering(("slow", slow, {})),
            LifecycleStep(id="teardown", shutdown=teardown),
            budget=timedelta(seconds=30),
        )

        async def main() -> None:
            async with runtime.scope():
                entered.set()
                await asyncio.sleep(60)

        task = asyncio.create_task(main())
        await asyncio.wait_for(entered.wait(), timeout=5.0)

        task.cancel()  # the first Ctrl+C: the scope exits, and shutdown drains
        await asyncio.wait_for(phase_started.wait(), timeout=5.0)

        task.cancel()  # the second, mid-phase

        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=10.0)

        assert events == ["phase-started", "phase-cancelled", ("teardown", True)]
        assert runtime.ready is False and runtime.draining is False  # the scope is gone

    # ....................... #

    @pytest.mark.asyncio
    async def test_shutdown_after_an_explicit_drain_does_not_run_the_phases_again(
        self,
    ) -> None:
        ran: list[str] = []

        async def phase(_allowed: timedelta) -> None:
            ran.append("phase")

        runtime = _runtime(_registering(("phase", phase, {})))

        async with runtime.scope():
            await runtime.drain(timedelta(seconds=1))

        assert ran == ["phase"]

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_partial_startup_failure_runs_no_phase(self) -> None:
        ran: list[str] = []

        async def phase(_allowed: timedelta) -> None:
            ran.append("phase")

        async def failing(_ctx: ExecutionContext) -> None:
            raise RuntimeError("startup failed")

        runtime = _runtime(
            _registering(("phase", phase, {})),
            LifecycleStep(id="failing", startup=failing, depends_on=("phases",)),
        )

        with pytest.raises(RuntimeError, match="startup failed"):
            async with runtime.scope():
                pass

        assert ran == []

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_drain_asked_for_during_startup_leaves_the_phases_to_shutdown(
        self,
    ) -> None:
        """A signal can arrive mid-startup. Nothing is drained or claimed yet."""

        ran: list[str] = []
        holder: list[ExecutionRuntime] = []

        async def phase(_allowed: timedelta) -> None:
            ran.append("phase")

        async def drains_early(_ctx: ExecutionContext) -> None:
            await holder[0].drain(timedelta(seconds=1))
            ran.append("early drain returned")

        runtime = _runtime(
            _registering(("phase", phase, {})),
            LifecycleStep(id="early", startup=drains_early, depends_on=("phases",)),
        )
        holder.append(runtime)

        async with runtime.scope():
            assert runtime.ready is True  # the early call did not announce anything

        assert ran == ["early drain returned", "phase"]

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_second_drain_waits_for_the_running_one(self) -> None:
        release = asyncio.Event()
        ran: list[str] = []

        async def phase(_allowed: timedelta) -> None:
            ran.append("phase")
            await release.wait()

        runtime = _runtime(_registering(("phase", phase, {})))

        async with runtime.scope():
            first = asyncio.create_task(runtime.drain(timedelta(seconds=5)))
            await asyncio.sleep(0.05)

            second = asyncio.create_task(runtime.drain(timedelta(seconds=5)))
            await asyncio.sleep(0.05)

            assert not second.done()

            release.set()
            await asyncio.wait_for(asyncio.gather(first, second), timeout=5.0)

        assert ran == ["phase"]


# ----------------------- #


class TestDrainPhaseRegistration:
    @pytest.mark.asyncio
    async def test_a_name_is_registered_once(self) -> None:
        async def phase(_allowed: timedelta) -> None:
            pass

        runtime = _runtime()

        async with runtime.scope():
            phases = runtime.get_context().drain_phases
            phases.register("phase", phase)

            with pytest.raises(CoreException) as caught:
                phases.register("phase", phase)

            assert caught.value.kind == ExceptionKind.CONFIGURATION

            # A cap that is not positive, or under the floor a phase is skipped below.
            for cap in (timedelta(0), timedelta(milliseconds=50)):
                with pytest.raises(CoreException) as caught:
                    phases.register("other", phase, max_duration=cap)

                assert caught.value.kind == ExceptionKind.CONFIGURATION

            phases.register("at the floor", phase, max_duration=timedelta(milliseconds=100))
