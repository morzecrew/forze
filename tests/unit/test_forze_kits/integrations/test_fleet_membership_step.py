"""The fleet membership step, run in its own runtime's plan."""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import Awaitable, Callable
from datetime import timedelta

import attrs
import pytest

from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.fleet import (
    FleetDepKey,
    FleetSpec,
    FleetView,
    Member,
    MemberState,
    Report,
)
from forze.application.execution import (
    DeploymentProfile,
    Deps,
    ExecutionRuntime,
    LifecyclePlan,
    build_runtime,
)
from forze.base.exceptions import CoreException
from forze.base.primitives import instance_id, utcnow
from forze_kits.integrations.fleet import fleet_membership_lifecycle_step
from forze_mock import MockDepsModule
from forze_mock.adapters import MockFleetAdapter
from forze_mock.state import MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #

SPEC = FleetSpec(name="gateways", ttl=timedelta(seconds=2))
EVERY = timedelta(milliseconds=20)


async def _until(check: Callable[[], Awaitable[bool]], *, timeout: float = 3.0) -> None:
    """Wait for *check*, failing after *timeout* rather than at the test's own timeout."""

    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    while not await check():
        if loop.time() > deadline:
            raise AssertionError("condition not reached")

        await asyncio.sleep(0.005)


def _observer(state: MockState) -> MockFleetAdapter:
    """A peer reading the same fleet."""

    return MockFleetAdapter(spec=SPEC, state=state, namespace=str(SPEC.name))


async def _state_of(observer: MockFleetAdapter, member_id: str) -> MemberState | None:
    view = await observer.view()

    return next((m.state for m in view.members if m.id == member_id), None)


# ....................... #


class TestStateFollowsTheScope:
    async def test_starting_until_startup_finishes_then_serving_then_draining(self) -> None:
        state = MockState()
        observer = _observer(state)
        beats = itertools.count()
        release, inside, leave = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def slow_startup(_ctx: object) -> None:
            await release.wait()

        runtime = build_runtime(
            MockDepsModule(state=state),
            lifecycle_steps=[
                fleet_membership_lifecycle_step(
                    SPEC, every=EVERY, member_id="me", report=lambda: {"beat": next(beats)}
                ),
                LifecycleStep(id="slow", startup=slow_startup, depends_on=("fleet_membership",)),
            ],
            drain_timeout=timedelta(0),
        )

        async def hold() -> None:
            async with runtime.scope():
                inside.set()
                await leave.wait()

        task = asyncio.create_task(hold())

        try:
            # The loop keeps beating while another step is still starting.
            await _until(lambda: _beaten_at_least(observer, "me", 4))
            assert await _state_of(observer, "me") == "starting"

            release.set()
            await asyncio.wait_for(inside.wait(), timeout=5.0)
            await _until(lambda: _is(observer, "me", "serving"))

            runtime.begin_draining()
            await _until(lambda: _is(observer, "me", "draining"))

        finally:
            release.set()
            leave.set()
            await asyncio.wait_for(task, timeout=10.0)

        # Leaving removes the entry at once.
        assert await _state_of(observer, "me") is None

    async def test_the_default_member_id_is_the_instance_id(self) -> None:
        state = MockState()
        runtime = build_runtime(
            MockDepsModule(state=state),
            lifecycle_steps=fleet_membership_lifecycle_step(SPEC, every=EVERY),
            drain_timeout=timedelta(0),
        )

        async with runtime.scope():
            assert await _state_of(_observer(state), instance_id()) is not None


async def _is(observer: MockFleetAdapter, member_id: str, state: MemberState) -> bool:
    return await _state_of(observer, member_id) == state


async def _beaten_at_least(observer: MockFleetAdapter, member_id: str, beats: int) -> bool:
    view = await observer.view()
    member = next((m for m in view.members if m.id == member_id), None)

    return member is not None and int(member.report["beat"]) >= beats


# ....................... #


class TestStop:
    async def test_a_linger_entry_outlives_the_process_then_expires_alone(self) -> None:
        state = MockState()
        observer = _observer(state)
        await observer.beat("peer", "serving", {}, ttl=timedelta(seconds=30))

        runtime = build_runtime(
            MockDepsModule(state=state),
            lifecycle_steps=fleet_membership_lifecycle_step(
                SPEC,
                every=EVERY,
                member_id="me",
                on_stop="linger",
                linger_for=timedelta(milliseconds=200),
            ),
            drain_timeout=timedelta(0),
        )

        async with runtime.scope():
            await _until(lambda: _is(observer, "me", "serving"))

        assert await _state_of(observer, "me") == "draining"

        async def gone() -> bool:
            return await _state_of(observer, "me") is None

        # Well inside the fleet's own ttl: the linger entry lives for linger_for alone.
        await _until(gone, timeout=1.0)
        assert await _state_of(observer, "peer") == "serving"

    async def test_a_hand_driven_shutdown_stops_the_loop_before_it_leaves(self) -> None:
        # No runtime stops the loop here, so the step's own shutdown must, or the loop's
        # next beat writes the entry back after the leave.
        state = MockState()
        observer = _observer(state)
        ctx = context_from_modules(MockDepsModule(state=state))
        plan = LifecyclePlan.from_steps(
            fleet_membership_lifecycle_step(SPEC, every=EVERY, member_id="me")
        ).freeze()

        await plan.startup(ctx)
        await _until(lambda: _is(observer, "me", "starting"))
        await plan.shutdown(ctx)
        await asyncio.sleep(0.1)  # five beat intervals

        assert await _state_of(observer, "me") is None


# ....................... #


@attrs.define
class _FlakyFleet:
    """A fleet port whose beats fail on the calls in *fail_on*, or hang on those in *hang_on*."""

    fail_on: frozenset[int] = frozenset()
    hang_on: frozenset[int] = frozenset()
    calls: int = 0
    left: list[str] = attrs.field(factory=list)
    cancelled: list[int] = attrs.field(factory=list)

    async def beat(
        self,
        member_id: str,
        state: MemberState,
        report: Report,
        *,
        ttl: timedelta | None = None,
    ) -> FleetView:
        self.calls += 1

        if self.calls in self.fail_on:
            raise RuntimeError(f"beat {self.calls} failed")

        if self.calls in self.hang_on:
            call = self.calls

            try:
                await asyncio.sleep(30)  # bounded: a lost timeout fails the test, not pytest

            except asyncio.CancelledError:
                self.cancelled.append(call)
                raise

        member = Member(id=member_id, state=state, since=utcnow(), report=report)

        return FleetView(members=[member], at=utcnow())

    async def view(self) -> FleetView:
        return FleetView(members=[], at=utcnow())

    async def leave(self, member_id: str) -> None:
        self.left.append(member_id)


def _flaky_runtime(port: _FlakyFleet, spec: FleetSpec = SPEC) -> ExecutionRuntime:
    return build_runtime(
        deps=Deps.plain({FleetDepKey: lambda _ctx, _spec: port}),
        lifecycle_steps=fleet_membership_lifecycle_step(spec, every=EVERY, member_id="me"),
        drain_timeout=timedelta(0),
    )


class TestSupervision:
    async def test_a_hung_beat_is_abandoned_and_the_loop_beats_on(self) -> None:
        # ttl 300 ms, every 20 ms: a beat may take 280 ms before it is abandoned.
        port = _FlakyFleet(hang_on=frozenset({2}))
        spec = FleetSpec(name="gateways", ttl=timedelta(milliseconds=300))

        async with _flaky_runtime(port, spec).scope():

            async def beat_again() -> bool:
                return port.calls >= 4

            await _until(beat_again, timeout=2.0)

        assert port.cancelled == [2]

    async def test_a_hung_first_beat_fails_startup(self) -> None:
        port = _FlakyFleet(hang_on=frozenset({1}))
        spec = FleetSpec(name="gateways", ttl=timedelta(milliseconds=300))

        with pytest.raises(CoreException) as caught:
            async with _flaky_runtime(port, spec).scope():
                pass

        assert caught.value.code == "fleet.beat_timeout"
        assert port.cancelled == [1]

    async def test_a_failed_beat_does_not_stop_the_loop(self) -> None:
        port = _FlakyFleet(fail_on=frozenset({2, 3}))

        async with _flaky_runtime(port).scope():

            async def beaten_past_failures() -> bool:
                return port.calls >= 6

            await _until(beaten_past_failures)

        assert port.left == ["me"]

        # The runtime stopped the loop: no beat lands after the scope closed.
        stopped_at = port.calls
        await asyncio.sleep(0.1)
        assert port.calls == stopped_at

    async def test_a_failed_first_beat_fails_startup(self) -> None:
        port = _FlakyFleet(fail_on=frozenset({1}))

        with pytest.raises(RuntimeError, match="beat 1 failed"):
            async with _flaky_runtime(port).scope():
                pass

        # Startup failed, so nothing shut it down, and no loop kept beating.
        await asyncio.sleep(0.1)
        assert (port.calls, port.left) == (1, [])


# ....................... #


class TestConstruction:
    @pytest.mark.parametrize("every", [timedelta(seconds=1), timedelta(seconds=3)])
    def test_every_of_half_the_ttl_or_more_is_refused(self, every: timedelta) -> None:
        with pytest.raises(CoreException, match="below half"):
            fleet_membership_lifecycle_step(SPEC, every=every)

    def test_every_just_below_half_the_ttl_is_accepted(self) -> None:
        fleet_membership_lifecycle_step(SPEC, every=timedelta(milliseconds=999))

    @pytest.mark.parametrize("on_stop", ["Leave", "stay", ""])
    def test_an_unknown_on_stop_is_refused(self, on_stop: str) -> None:
        with pytest.raises(CoreException, match="on_stop"):
            fleet_membership_lifecycle_step(SPEC, every=EVERY, on_stop=on_stop)  # type: ignore[arg-type]

    def test_a_linger_needs_a_lifetime(self) -> None:
        with pytest.raises(CoreException, match="linger"):
            fleet_membership_lifecycle_step(SPEC, every=EVERY, on_stop="linger")

    def test_an_invalid_member_id_is_refused_at_wiring(self) -> None:
        with pytest.raises(CoreException) as caught:
            fleet_membership_lifecycle_step(SPEC, every=EVERY, member_id="a\tb")

        assert caught.value.code == "fleet.member_id_invalid"

    def test_markers(self) -> None:
        step = fleet_membership_lifecycle_step(SPEC, every=EVERY, depends_on=("redis",))

        assert step.requires_long_running is True
        assert step.mutates_shared_state is False
        assert step.singleton_guarded is False
        assert step.depends_on == ("redis",)

    def test_the_fleet_profile_accepts_it_unguarded(self) -> None:
        build_runtime(
            MockDepsModule(),
            lifecycle_steps=fleet_membership_lifecycle_step(SPEC, every=EVERY),
            deployment=DeploymentProfile.FLEET,
        )

    def test_the_serverless_profile_refuses_it(self) -> None:
        with pytest.raises(CoreException, match="SERVERLESS"):
            build_runtime(
                MockDepsModule(),
                lifecycle_steps=fleet_membership_lifecycle_step(SPEC, every=EVERY),
                deployment=DeploymentProfile.SERVERLESS,
            )
