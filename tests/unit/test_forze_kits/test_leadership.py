"""Lifetime leadership over the in-memory lock, on the simulation loop's virtual clock.

The mock lock reads its expiries from ``monotonic()``, which the simulation binds to the loop's
clock, so the step's deadlines and the lock's TTLs run on one clock: a two-minute scenario takes
milliseconds, and every time below is exact.

Each replica is its own step and context over one shared ``MockState`` (the "Redis" the fleet
shares), with its own fault switches on the lock.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

import attrs
import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from forze.application.contracts.dlock import (
    FENCE_MAX_VALUE,
    AcquiredLock,
    DistributedLockCapabilities,
    DistributedLockCommandDepKey,
    DistributedLockQueryDepKey,
    DistributedLockSpec,
)
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import DeploymentProfile, Deps
from forze.application.execution.context.drainable import Drainables
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import monotonic
from forze.testing import context_from_deps
from forze_dst.runtime import run_simulation
from forze_kits.lifecycle import (
    LeadershipSpec,
    Term,
    instrument_leadership,
    leadership_lifecycle_step,
)
from forze_mock import MockDepsModule
from forze_mock.adapters.dlock import MockDistributedLockAdapter
from forze_mock.state import MockState

# ----------------------- #

KEY = "relay"
LOCK = DistributedLockSpec(name="leader", requires_fencing_token=True)  # ttl 30s
SPEC = LeadershipSpec(lock=LOCK, key=KEY)
"""Defaults: ttl 30, renew every 10, campaign every 2, margin 1, grace 5, release 1."""

TTL, RENEW, CAMPAIGN, MARGIN, GRACE, RELEASE = 30.0, 10.0, 2.0, 1.0, 5.0, 1.0

_DEFER = 3
"""Campaign intervals a replica defers to an asker while it sees the lease free."""

FOREVER = 3600.0
"""A hang, bounded: an hour of virtual time, far past every scenario's end."""

Lead = Callable[[Any, Term], Awaitable[None]]


# ....................... #


@attrs.define(slots=True, eq=False)
class _Lock:
    """The shared mock lock, with faults one replica switches on for its own calls."""

    inner: MockDistributedLockAdapter
    reset_plan: list[float | None] = attrs.field(factory=list)
    """Per reset call, in order: answer after this many seconds, or ``None`` to hang."""
    acquire_delay: float = 0.0
    """Seconds an acquire of the lease takes to land; a request on the wire lands even when
    its caller is cancelled."""
    want_delay: float = 0.0
    """The same, for an acquire of the want-lock."""
    lands_then_raises: int = 0
    """How many lease acquires land and then raise, as a reset connection does."""
    acquire_fails_on: frozenset[str] = frozenset()
    release_fails: bool = False
    release_delay: float = 0.0
    reads_fail: bool = False
    """Owner and ttl reads raise, as on a broken connection."""
    read_delay: float = 0.0
    """Seconds every read takes to answer."""
    late_reply_raises: float = 0.0
    """The next lease acquire takes the lock at once, and its reply fails this much later."""
    acquire_fails_at: frozenset[int] = frozenset()
    answers_none: bool = False
    """Lease acquires answer ``None`` without taking the lock, as if it were always held."""
    """Which lease acquires, counted from one, raise without taking the lock."""
    lease_acquires: int = 0
    log: list[tuple[float, str, str, str]] = attrs.field(factory=list)

    @property
    def spec(self) -> DistributedLockSpec:
        return self.inner.spec

    def calls(self, op: str, key: str = KEY) -> list[tuple[float, str]]:
        return [(at, owner) for at, o, k, owner in self.log if o == op and k == key]

    async def acquire(self, key: str, owner: str) -> AcquiredLock | None:
        self.log.append((monotonic(), "acquire", key, owner))

        if key in self.acquire_fails_on:
            raise exc.infrastructure("acquire refused")

        if key == KEY:
            self.lease_acquires += 1

            if self.lease_acquires in self.acquire_fails_at:
                raise exc.infrastructure("acquire refused")

            if self.answers_none:
                return None

        if key == KEY and self.late_reply_raises:
            delay, self.late_reply_raises = self.late_reply_raises, 0.0
            return await asyncio.shield(
                asyncio.ensure_future(self._land_then_fail(delay, key, owner))
            )

        delay = self.acquire_delay if key == KEY else self.want_delay

        if delay:
            # Shielded: cancelling the caller does not stop a request already sent.
            return await asyncio.shield(asyncio.ensure_future(self._land(delay, key, owner)))

        acquired = await self.inner.acquire(key, owner)

        if key == KEY and self.lands_then_raises:
            self.lands_then_raises -= 1
            raise exc.infrastructure("connection reset after the lock was taken")

        return acquired

    async def _land(self, delay: float, key: str, owner: str) -> AcquiredLock | None:
        await asyncio.sleep(delay)

        return await self.inner.acquire(key, owner)

    async def _land_then_fail(self, delay: float, key: str, owner: str) -> AcquiredLock | None:
        await self.inner.acquire(key, owner)
        await asyncio.sleep(delay)

        raise exc.infrastructure("the reply was lost after the lock was taken")

    async def reset(self, key: str, owner: str) -> bool:
        self.log.append((monotonic(), "reset", key, owner))

        if key == KEY and self.reset_plan:
            delay = self.reset_plan.pop(0)
            await asyncio.sleep(FOREVER if delay is None else delay)

        return await self.inner.reset(key, owner)

    async def release(self, key: str, owner: str) -> bool:
        self.log.append((monotonic(), "release", key, owner))

        if self.release_fails:
            raise exc.infrastructure("release refused")

        if self.release_delay:
            await asyncio.sleep(self.release_delay)

        return await self.inner.release(key, owner)

    async def is_locked(self, key: str) -> bool:
        await asyncio.sleep(self.read_delay)

        return await self.inner.is_locked(key)

    async def get_owner(self, key: str) -> str | None:
        await asyncio.sleep(self.read_delay)

        if self.reads_fail:
            raise exc.infrastructure("read refused")

        return await self.inner.get_owner(key)

    async def get_ttl(self, key: str) -> timedelta | None:
        await asyncio.sleep(self.read_delay)

        return await self.inner.get_ttl(key)

    async def advance_fence(self, key: str, *, past: int) -> int:
        self.log.append((monotonic(), "advance", key, str(past)))

        return await self.inner.advance_fence(key, past=past)


@attrs.define(slots=True, eq=False)
class _Dlock:
    lock: _Lock

    def command(self, _spec: DistributedLockSpec) -> _Lock:
        return self.lock

    def query(self, _spec: DistributedLockSpec) -> _Lock:
        return self.lock


@attrs.define(slots=True, eq=False)
class _Ctx:
    """The two things the step reads off a context: its lock ports and its drainables."""

    dlock: _Dlock
    drainables: Drainables = attrs.field(factory=Drainables)


# ....................... #


@attrs.define(slots=True, eq=False)
class _Run:
    """One body's life, as the body itself saw it."""

    replica: str
    term: Term
    started: float
    saw_end: float | None = None
    cancelled: float | None = None
    finished: float | None = None


@attrs.define(slots=True, eq=False)
class _Replica:
    name: str
    lock: _Lock
    step: LifecycleStep
    ctx: _Ctx

    async def start(self) -> None:
        await self.step.startup(self.ctx)  # type: ignore[arg-type]

    async def stop(self) -> None:
        await self.ctx.drainables.stop_all(grace=60)

    def kill(self) -> None:
        """The process dies: the loop and its body are cancelled, nothing is released."""

        self.loop_task().cancel()

    def loop_task(self) -> asyncio.Task[None]:
        task = self.step.startup.control.task  # type: ignore[attr-defined]
        assert task is not None
        return task


@attrs.define(slots=True, eq=False)
class _Fleet:
    state: MockState = attrs.field(factory=MockState)
    runs: list[_Run] = attrs.field(factory=list)
    replicas: list[_Replica] = attrs.field(factory=list)

    def lock(self) -> _Lock:
        return _Lock(
            inner=MockDistributedLockAdapter(spec=LOCK, state=self.state, namespace="leader")
        )

    def body(self, name: str, *, listens: bool = True, resists: float = 0.0) -> Lead:
        """A body that ends at its term's end, or ignores it; *resists* swallows the cancel."""

        async def lead(_ctx: Any, term: Term) -> None:
            run = _Run(replica=name, term=term, started=monotonic())
            self.runs.append(run)

            try:
                if listens:
                    await term.ended.wait()
                    run.saw_end = monotonic()

                else:
                    await asyncio.sleep(FOREVER)

            except asyncio.CancelledError:
                run.cancelled = monotonic()

                if not resists:
                    run.finished = monotonic()
                    raise

                await asyncio.sleep(resists)

            run.finished = monotonic()

        return lead

    def replica(
        self,
        name: str,
        *,
        owner: str | None = None,
        lead: Lead | None = None,
        spec: LeadershipSpec = SPEC,
        **step_kw: Any,
    ) -> _Replica:
        lock = self.lock()
        step = leadership_lifecycle_step(
            spec,
            owner=owner or name,
            lead=lead or self.body(name),
            **step_kw,
        )
        replica = _Replica(name=name, lock=lock, step=step, ctx=_Ctx(dlock=_Dlock(lock=lock)))
        self.replicas.append(replica)

        return replica

    async def stop_all(self) -> None:
        await asyncio.gather(*(r.stop() for r in self.replicas))

    def by(self, name: str) -> list[_Run]:
        return [run for run in self.runs if run.replica == name]

    def assert_one_at_a_time(self) -> None:
        spans = sorted(
            (run.started, run.finished if run.finished is not None else math.inf)
            for run in self.runs
        )

        for (_, first_end), (second_start, _) in zip(spans, spans[1:]):
            assert second_start >= first_end, spans


def _simulate(scenario: Callable[[], Awaitable[None]]) -> None:
    run_simulation(scenario)


def _first_acquire(replica: _Replica) -> float:
    return replica.lock.calls("acquire")[0][0]


# ----------------------- #


class TestElection:
    def test_one_leader_among_three(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            for name in ("a", "b", "c"):
                await fleet.replica(name).start()

            await asyncio.sleep(120)

            assert len(fleet.runs) == 1
            assert fleet.runs[0].term.live()

            await fleet.stop_all()

            assert fleet.runs[0].term.reason == "stopped"

        _simulate(scenario)

    def test_a_stopped_leader_hands_over_within_a_campaign_interval(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            await a.start()
            await asyncio.sleep(5)
            await fleet.replica("b").start()
            await asyncio.sleep(30)

            stopped_at = monotonic()
            await a.stop()

            (leader,) = fleet.by("a")
            assert leader.term.reason == "stopped"
            assert leader.finished == stopped_at  # the body ended on its term's end
            released_at = a.lock.calls("release")[-1][0]
            assert released_at >= leader.finished

            await asyncio.sleep(10)

            (successor,) = fleet.by("b")
            assert successor.started - released_at <= CAMPAIGN
            assert successor.term.token is not None and leader.term.token is not None
            assert successor.term.token > leader.term.token

            fleet.assert_one_at_a_time()
            await fleet.stop_all()

        _simulate(scenario)

    def test_a_killed_leader_is_replaced_within_ttl_plus_a_campaign_interval(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            await a.start()
            await asyncio.sleep(1)
            await fleet.replica("b").start()
            await asyncio.sleep(25)

            killed_at = monotonic()
            a.kill()
            await asyncio.sleep(0)

            assert not a.lock.calls("release")  # a dead process releases nothing
            (killed,) = fleet.by("a")
            assert killed.term.reason == "stopped" and not killed.term.live()
            assert a.step.startup.state.terms_ended == {"stopped": 1}  # type: ignore[attr-defined]
            last_renewal = max(at for at, _ in a.lock.calls("reset"))

            await asyncio.sleep(TTL + 2 * CAMPAIGN)

            (successor,) = fleet.by("b")
            assert successor.started >= last_renewal + TTL  # never before the lease lapsed
            assert successor.started - killed_at <= TTL + CAMPAIGN

            await fleet.stop_all()

        _simulate(scenario)


# ....................... #


class TestDeadlines:
    def test_hanging_renewals_end_the_term_at_the_soft_deadline(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            a.lock.reset_plan = [None] * 10
            await a.start()
            await fleet.replica("b").start()
            await asyncio.sleep(60)

            sent = _first_acquire(a)
            leader, successor, *_ = fleet.runs
            assert leader.replica == "a"
            assert leader.term.reason == "expiring"

            # The renewal sent at +10 is bounded by the time left to the soft deadline, so it
            # gives up exactly there; the body is told then and is gone before the hard one.
            assert leader.saw_end == sent + TTL - MARGIN - GRACE
            assert leader.finished is not None
            assert leader.finished <= sent + TTL - MARGIN
            assert successor.started >= leader.finished
            fleet.assert_one_at_a_time()

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_body_that_ignores_its_end_is_cancelled_before_the_hard_deadline(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", lead=fleet.body("a", listens=False))
            a.lock.reset_plan = [None] * 10
            await a.start()
            await fleet.replica("b").start()
            await asyncio.sleep(60)

            sent = _first_acquire(a)
            leader, successor, *_ = fleet.runs
            assert leader.replica == "a"
            assert leader.cancelled == sent + TTL - MARGIN - RELEASE
            assert successor.started >= leader.cancelled
            fleet.assert_one_at_a_time()

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_slow_acquire_shortens_the_term(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            a.lock.acquire_delay = 3.0  # lands three seconds after it was sent
            a.lock.reset_plan = [None] * 10
            await a.start()
            await asyncio.sleep(60)

            sent = _first_acquire(a)
            (leader, *_) = fleet.runs
            assert leader.started == sent + 3.0
            assert leader.term.reason == "expiring"
            # Measured from the send, not from the answer three seconds later.
            assert leader.saw_end == sent + TTL - MARGIN - GRACE

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_cancelled_body_unwinds_inside_the_lease_with_no_margin(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            spec = attrs.evolve(SPEC, margin=timedelta(0))
            a = fleet.replica("a", spec=spec, lead=fleet.body("a", listens=False, resists=0.5))
            a.lock.reset_plan = [None] * 10
            await a.start()
            await fleet.replica("b", spec=spec).start()
            await asyncio.sleep(60)

            hard = _first_acquire(a) + TTL  # no margin: the lease itself
            leader = fleet.by("a")[0]
            assert leader.cancelled == hard - RELEASE
            assert leader.finished is not None and leader.finished <= hard
            fleet.assert_one_at_a_time()

            await fleet.stop_all()

        _simulate(scenario)

    def test_an_unwind_of_the_whole_release_timeout_leaves_the_margin_intact(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", lead=fleet.body("a", listens=False, resists=RELEASE))
            a.lock.reset_plan = [None] * 10
            await a.start()
            await fleet.replica("b").start()
            await asyncio.sleep(60)

            hard = _first_acquire(a) + TTL - MARGIN
            leader = fleet.by("a")[0]
            # Done by the hard deadline, the margin untouched. At exactly the bound the step may
            # not see it finish, and leaves the lease to lapse; either way nothing overlaps.
            assert leader.finished == hard
            fleet.assert_one_at_a_time()

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_stop_near_the_soft_deadline_is_cut_short_of_the_hard_one(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", lead=fleet.body("a", listens=False, resists=0.9))
            a.lock.reset_plan = [None] * 10
            await a.start()
            await asyncio.sleep(23.5)  # the renewal at +10 hangs; the soft deadline is +24
            sent = _first_acquire(a)

            await a.stop()

            (run,) = fleet.runs
            assert run.term.reason == "stopped"
            # Not +28.5: the grace is cut where a cancelled body still finishes in the lease.
            assert run.cancelled == sent + TTL - MARGIN - RELEASE
            assert run.finished is not None and run.finished <= sent + TTL - MARGIN

        _simulate(scenario)

    def test_a_body_that_sets_its_own_end_is_not_cut_short(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def lead(_ctx: Any, term: Term) -> None:
                run = _Run(replica="a", term=term, started=monotonic())
                fleet.runs.append(run)
                await asyncio.sleep(1)
                term.ended.set()  # tells the body's own workers to stop

                try:
                    await asyncio.sleep(15)  # drains them, past the next renewal

                except asyncio.CancelledError:
                    run.cancelled = monotonic()
                    raise

                run.finished = monotonic()

            a = fleet.replica("a", lead=lead)
            await a.start()
            await asyncio.sleep(17)

            (run,) = fleet.runs  # the next term starts at the next campaign, at +18
            assert run.cancelled is None
            assert run.finished == 16.0
            assert run.term.reason == "returned"
            assert a.lock.calls("release")[0][0] == 16.0

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_slow_renewal_shortens_the_term(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            # The renewal sent at +10 answers at +13; the next one hangs.
            a.lock.reset_plan = [3.0, None, None]
            await a.start()
            await asyncio.sleep(60)

            sent = _first_acquire(a)
            renewal_sent = sent + RENEW
            (leader, *_) = fleet.runs
            assert leader.term.reason == "expiring"
            # Measured from when the renewal was sent, not from its answer three seconds later.
            assert leader.saw_end == renewal_sent + TTL - MARGIN - GRACE

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_deleted_lease_cancels_the_body_at_the_next_renewal(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", lead=fleet.body("a", listens=False))
            await a.start()
            await asyncio.sleep(3)

            (leader,) = fleet.runs
            assert await a.lock.inner.release(KEY, leader.term.owner)  # the key is deleted

            await asyncio.sleep(10)

            assert leader.term.reason == "lost"
            renewed_at = a.lock.calls("reset")[0][0]
            assert leader.cancelled == renewed_at  # at once, no grace
            assert not a.lock.calls("release")  # the lease is not this term's to release

            await fleet.stop_all()

        _simulate(scenario)


# ....................... #


class TestRelease:
    def test_a_body_that_will_not_finish_keeps_the_lease_until_its_ttl(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", lead=fleet.body("a", listens=False, resists=200))
            await a.start()
            await asyncio.sleep(1)
            await fleet.replica("b").start()
            await asyncio.sleep(14)

            (stubborn,) = fleet.runs
            stopped_at = monotonic()
            await a.stop()

            # Cancelled after the grace, and still running past the release timeout.
            assert stubborn.cancelled == stopped_at + GRACE
            assert stubborn.finished is None
            assert monotonic() >= stopped_at + GRACE + RELEASE
            assert not a.lock.calls("release")
            assert await a.lock.inner.get_owner(KEY) == stubborn.term.owner

            await asyncio.sleep(TTL + 2 * CAMPAIGN)

            last_renewal = max(at for at, _ in a.lock.calls("reset"))
            (successor,) = fleet.by("b")
            assert successor.started >= last_renewal + TTL

            await fleet.stop_all()

        _simulate(scenario)

    def test_renewals_continue_through_a_stopped_terms_grace(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            spec = attrs.evolve(SPEC, renew_every=timedelta(seconds=2))
            a = fleet.replica("a", spec=spec, lead=fleet.body("a", listens=False))
            await a.start()
            await asyncio.sleep(3)

            stopped_at = monotonic()
            await a.stop()

            (run,) = fleet.runs
            assert run.cancelled == stopped_at + GRACE
            in_grace = [at for at, _ in a.lock.calls("reset") if stopped_at < at < run.cancelled]
            assert len(in_grace) >= 2
            assert a.lock.calls("release")[-1][0] == run.cancelled

        _simulate(scenario)

    def test_a_body_that_unwinds_within_the_release_timeout_is_released(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            # Ignores its end, and takes half a second to unwind once cancelled: the loop's
            # stop grace must cover the yield grace, that unwind and the release.
            a = fleet.replica("a", lead=fleet.body("a", listens=False, resists=0.5))
            await a.start()
            await asyncio.sleep(3)

            stopped_at = monotonic()
            await a.stop()

            (run,) = fleet.runs
            assert run.cancelled == stopped_at + GRACE
            assert run.finished == stopped_at + GRACE + 0.5
            assert a.lock.calls("release")[-1][0] == run.finished
            assert await a.lock.inner.is_locked(KEY) is False

        _simulate(scenario)

    def test_a_replica_does_not_lead_beside_a_body_it_abandoned(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", lead=fleet.body("a", listens=False, resists=100))
            a.lock.reset_plan = [None] * 10
            await a.start()
            await asyncio.sleep(200)

            first, second = fleet.runs
            assert first.cancelled == _first_acquire(a) + TTL - MARGIN - RELEASE
            assert not a.lock.calls("release")  # abandoned, so left to lapse
            assert first.finished is not None
            assert second.started >= first.finished

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_late_acquire_is_released_when_it_lands(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            a.lock.acquire_delay = 40.0  # past the wait bound of ttl - margin - grace
            await a.start()
            await asyncio.sleep(41)
            await fleet.replica("b").start()
            await asyncio.sleep(10)

            sent, first_owner = a.lock.calls("acquire")[0]
            landed = sent + 40.0
            assert (landed, first_owner) in a.lock.calls("release")
            assert not fleet.by("a")
            (successor,) = fleet.by("b")
            assert successor.started <= landed + 1 + CAMPAIGN

            await fleet.stop_all()

        _simulate(scenario)


# ....................... #


    def test_a_late_reply_that_raises_is_still_released(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            a.lock.late_reply_raises = 25.0  # takes the lock at once; the reply fails at +25
            await a.start()
            await asyncio.sleep(1)
            await fleet.replica("b").start()
            await asyncio.sleep(TTL + 2 * CAMPAIGN)

            sent, owner = a.lock.calls("acquire")[0]
            assert (sent + 25.0, owner) in a.lock.calls("release")
            first = fleet.runs[0]  # either replica: the lease was free again at +25
            assert first.started <= sent + 25.0 + CAMPAIGN

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_forced_stop_cancels_a_campaign_callback_in_flight(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            seen: list[str] = []

            async def gate(_ctx: Any) -> bool:
                try:
                    await asyncio.sleep(0.5)

                except asyncio.CancelledError:
                    seen.append("cancelled")
                    raise

                seen.append("finished")
                return True

            a = fleet.replica("a", may_campaign=gate)
            await a.start()
            await asyncio.sleep(0.1)
            a.kill()
            await asyncio.sleep(5)

            assert seen == ["cancelled"]

        _simulate(scenario)

    def test_an_acquire_in_flight_at_a_stop_is_released_when_it_lands(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            a.lock.acquire_delay = 5.0
            await a.start()
            await asyncio.sleep(1)
            await a.stop()

            assert a.loop_task().done()
            await asyncio.sleep(5)

            sent, owner = a.lock.calls("acquire")[0]
            assert (sent + 5.0, owner) in a.lock.calls("release")
            assert await a.lock.inner.is_locked(KEY) is False
            assert not fleet.runs

        _simulate(scenario)

    def test_an_acquire_in_flight_at_a_kill_is_released_when_it_lands(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            a.lock.acquire_delay = 5.0
            await a.start()
            await asyncio.sleep(1)
            a.kill()
            await asyncio.sleep(5)

            sent, owner = a.lock.calls("acquire")[0]
            assert (sent + 5.0, owner) in a.lock.calls("release")
            assert await a.lock.inner.is_locked(KEY) is False

        _simulate(scenario)

    def test_an_acquire_that_lands_then_raises_is_released_at_once(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            a.lock.lands_then_raises = 1
            await a.start()
            await asyncio.sleep(0.5)
            await fleet.replica("b").start()
            await asyncio.sleep(TTL + 3 * CAMPAIGN)

            raised_at, owner = a.lock.calls("acquire")[0]
            assert (raised_at, owner) in a.lock.calls("release")
            first = fleet.runs[0]
            assert first.started <= 2 * CAMPAIGN + 0.5  # not a whole ttl later

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_stop_finishes_the_unwind_and_the_release(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            spec = attrs.evolve(
                SPEC,
                release_timeout=timedelta(seconds=3),
                renew_every=timedelta(seconds=5),
            )
            a = fleet.replica("a", spec=spec, lead=fleet.body("a", listens=False, resists=2.9))
            a.lock.release_delay = 2.0
            await a.start()
            await asyncio.sleep(1)

            await a.ctx.drainables.stop_all(grace=60)

            (run,) = fleet.runs
            state = a.step.startup.state  # type: ignore[attr-defined]
            assert run.finished is not None
            assert a.lock.calls("release")[-1][0] == run.finished
            assert await a.lock.inner.get_owner(KEY) is None  # the release went through
            assert not state.holding
            assert state.terms_ended == {"stopped": 1}

        _simulate(scenario)

    def test_a_body_abandoned_by_one_startup_holds_off_the_next(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", lead=fleet.body("a", listens=False, resists=100))
            await a.start()
            await asyncio.sleep(1)

            a.kill()  # the body swallows its cancellation and runs on
            await asyncio.sleep(0)
            a.ctx = _Ctx(dlock=_Dlock(lock=a.lock))
            await a.start()  # the step starts again in the same process
            await asyncio.sleep(150)

            first, second = fleet.runs
            assert first.finished is not None
            assert second.started >= first.finished

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_hanging_campaign_callback_does_not_hold_up_a_stop(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def never(_ctx: Any) -> bool:
                await asyncio.sleep(FOREVER)
                return True

            a = fleet.replica("a", may_campaign=never)
            await a.start()
            await asyncio.sleep(0.5)

            stopped_at = monotonic()
            await a.stop()

            assert a.loop_task().done()
            assert monotonic() - stopped_at < RELEASE

        _simulate(scenario)

    def test_a_failed_release_leaves_the_lease_to_lapse(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            started: list[float] = []

            async def lead(_ctx: Any, _term: Term) -> None:
                started.append(monotonic())
                await asyncio.sleep(1)

            a = fleet.replica("a", lead=lead)
            a.lock.release_fails = True
            await a.start()
            await asyncio.sleep(TTL + 2 * CAMPAIGN)

            released_at = a.lock.calls("release")[0][0]
            attempts = [at for at, _ in a.lock.calls("acquire")]
            # The failure is the release's alone: the campaign keeps its own pace, with no
            # supervisor backoff, and the lease lapses at its ttl instead.
            assert attempts[1] == released_at + CAMPAIGN
            assert len(started) == 2
            assert started[1] >= started[0] + TTL

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_body_that_raises_after_its_term_ended_is_still_released(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def lead(_ctx: Any, term: Term) -> None:
                await term.ended.wait()
                raise RuntimeError("raised while winding down")

            a = fleet.replica("a", lead=lead)
            await a.start()
            await asyncio.sleep(1)
            await a.stop()

            assert a.loop_task().done()
            assert a.lock.calls("release")
            assert await a.lock.inner.is_locked(KEY) is False

        _simulate(scenario)


class TestOwnersAndTokens:
    def test_replicas_sharing_an_owner_never_both_lead(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", owner="pod")
            await a.start()
            await asyncio.sleep(1)
            await fleet.replica("b", owner="pod").start()
            await fleet.replica("c", owner="pod").start()
            await asyncio.sleep(20)

            a.kill()
            await asyncio.sleep(TTL + 2 * CAMPAIGN)

            old, new = fleet.runs
            assert old.term.owner != new.term.owner
            assert old.term.owner.startswith("pod:") and new.term.owner.startswith("pod:")
            # The old term's late release cannot touch the successor's lease.
            assert await a.lock.inner.release(KEY, old.term.owner) is False
            assert await a.lock.inner.get_owner(KEY) == new.term.owner
            fleet.assert_one_at_a_time()

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_repeated_token_is_claimed_once(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            fence = {"value": 0}
            claimed: list[tuple[str, int | None, bool]] = []

            def claim_for(name: str) -> Callable[[Any, Term], Awaitable[bool]]:
                async def claim(_ctx: Any, term: Term) -> bool:
                    assert term.token is not None
                    ok = fence["value"] < term.token  # the store's claim rule
                    fence["value"] = max(fence["value"], term.token)
                    claimed.append((name, term.token, ok))
                    return ok

                return claim

            a = fleet.replica("a", claim=claim_for("a"))
            await a.start()
            await asyncio.sleep(1)

            # The lock backend loses its data: the lease and the fencing counter.
            fleet.state.dlocks.clear()
            fleet.state.dlock_fences.clear()
            await fleet.replica("b", claim=claim_for("b")).start()
            await asyncio.sleep(30)

            assert claimed[:3] == [("a", 1, True), ("b", 1, False), ("b", 2, True)]
            assert [(run.replica, run.term.token) for run in fleet.runs] == [("a", 1), ("b", 2)]
            assert fleet.by("a")[0].term.reason == "lost"

            await fleet.stop_all()

        _simulate(scenario)

    def test_verify_reads_the_owner(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            stolen = asyncio.Event()
            seen: list[bool] = []

            async def lead(_ctx: Any, term: Term) -> None:
                seen.append(await term.held())
                await stolen.wait()
                seen.append(await term.held())
                seen.append(term.live())

            a = fleet.replica("a", lead=lead, spec=attrs.evolve(SPEC, verify=True))
            await a.start()
            await asyncio.sleep(1)
            await a.lock.inner.release(KEY, a.lock.calls("acquire")[0][1])
            await a.lock.inner.acquire(KEY, "intruder")
            stolen.set()
            await asyncio.sleep(1)

            assert seen == [True, False, True]

            await fleet.stop_all()

        _simulate(scenario)


# ....................... #


class TestSupervision:
    def test_a_raising_body_is_released_and_backed_off(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            started: list[float] = []

            async def lead(_ctx: Any, _term: Term) -> None:
                started.append(monotonic())
                raise RuntimeError("boom")

            a = fleet.replica("a", lead=lead)
            await a.start()
            await asyncio.sleep(20)

            assert len(started) >= 2
            releases = a.lock.calls("release")
            assert releases and releases[0][0] == started[0]
            assert started[1] - started[0] >= 5  # the supervisor's restart backoff

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_configuration_error_in_the_body_stops_campaigning(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            started: list[float] = []

            async def lead(_ctx: Any, _term: Term) -> None:
                started.append(monotonic())
                raise exc.configuration("misconfigured")

            a = fleet.replica("a", lead=lead)
            await a.start()
            await asyncio.sleep(60)

            assert len(started) == 1
            assert a.loop_task().done()
            assert a.lock.calls("release")
            assert await a.lock.inner.is_locked(KEY) is False

            await fleet.stop_all()

        _simulate(scenario)


# ....................... #


class TestHandoff:
    def _prefers(self, value: bool) -> Callable[[Any], Awaitable[bool]]:
        async def prefer(_ctx: Any) -> bool:
            return value

        return prefer

    def test_a_preferring_replica_takes_the_unit_at_the_holders_next_renewal(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", handoff="on_request", prefer=self._prefers(False))
            await a.start()
            await asyncio.sleep(5)
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            await b.start()
            await asyncio.sleep(20)

            holder, asker = fleet.runs
            assert holder.replica == "a" and asker.replica == "b"
            assert holder.term.reason == "yielded"
            assert holder.saw_end == _first_acquire(a) + RENEW  # its next renewal
            assert asker.started - holder.saw_end <= CAMPAIGN
            assert b.lock.calls("release", f"{KEY}:want")  # given up once it leads
            assert await b.lock.inner.get_owner(f"{KEY}:want") is None

            await asyncio.sleep(60)
            assert len(fleet.runs) == 2  # and it is not handed back

            await fleet.stop_all()

        _simulate(scenario)

    def test_other_replicas_defer_to_the_asker(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", handoff="on_request", prefer=self._prefers(False))
            await a.start()
            await asyncio.sleep(0.5)
            # Ticks at +0.5, +2.5, ... +10.5: the first to see the lease free after the holder
            # yields at +10, ahead of the asker's tick at +11.
            n = fleet.replica("n", handoff="on_request", prefer=self._prefers(False))
            await n.start()
            await asyncio.sleep(4.5)
            await fleet.replica("b", handoff="on_request", prefer=self._prefers(True)).start()
            await asyncio.sleep(20)

            assert [run.replica for run in fleet.runs] == ["a", "b"]
            yielded_at = fleet.runs[0].saw_end
            assert yielded_at is not None
            # n's tick at +10.5 saw the lease free and the want-lock held, and sat out.
            assert not [at for at, _ in n.lock.calls("acquire") if yielded_at < at < yielded_at + 1]

            await fleet.stop_all()

        _simulate(scenario)

    def test_the_want_lock_rides_the_string_the_next_term_acquires_under(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            await fleet.replica("a", handoff="on_request", prefer=self._prefers(True)).start()
            await asyncio.sleep(1)
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            b.lock.acquire_delay = 30.0  # past the wait bound: the string is retired
            await b.start()
            await asyncio.sleep(2)
            b.lock.acquire_delay = 0.0  # the next acquires answer at once
            await asyncio.sleep(70)  # b gave up at +25; it asks again after a ttl

            wants = [owner for _, owner in b.lock.calls("acquire", f"{KEY}:want")]
            leases = [owner for _, owner in b.lock.calls("acquire")]
            assert len(set(wants)) >= 2
            assert all(want in leases for want in wants)  # each ask under a string it acquires with
            assert (b.lock.calls("release", f"{KEY}:want")[0][1]) == wants[0]

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_hanging_asker_does_not_leave_the_fleet_unled(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            await fleet.replica("a", handoff="on_request", prefer=self._prefers(False)).start()
            await asyncio.sleep(9)
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            b.lock.acquire_delay = 1000.0  # asks, then hangs on every lease acquire
            await b.start()
            await asyncio.sleep(40)

            yielded, second, *_ = fleet.runs
            assert yielded.term.reason == "yielded"
            assert yielded.finished is not None
            # The others defer while b holds the want-lock, but only for three campaign
            # intervals of seeing the lease free, the first of them up to one interval late.
            assert second.started - yielded.finished <= (3 + 1) * CAMPAIGN

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_former_holder_counts_the_free_lease_afresh(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            await fleet.replica("a", handoff="on_request", prefer=self._prefers(False)).start()
            await asyncio.sleep(9)
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            b.lock.answers_none = True  # keeps asking, and never takes the lease
            await b.start()
            await asyncio.sleep(60)

            first, second, third, *_ = fleet.by("a")
            assert first.term.reason == second.term.reason == "yielded"
            assert second.finished is not None
            # a deferred, escaped and led before its second yield; seeing the lease free after
            # it starts a new count rather than resuming the one from before that term.
            assert third.started - second.finished >= _DEFER * CAMPAIGN

            await fleet.stop_all()

        _simulate(scenario)

    def test_the_deferral_count_restarts_after_a_held_lease(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            await fleet.replica("a", handoff="on_request", prefer=self._prefers(False)).start()
            await asyncio.sleep(0.5)
            await fleet.replica("n", handoff="on_request", prefer=self._prefers(False)).start()
            await asyncio.sleep(8.5)
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            await b.start()  # +9: asks; a yields at +10; b leads at +11 and prefers it
            await asyncio.sleep(11)
            await fleet.replica("c", handoff="on_request", prefer=self._prefers(True)).start()
            await asyncio.sleep(10)
            await b.stop()  # +30: b releases while c asks
            await asyncio.sleep(20)

            # n saw the lease free once at +10.5, then held for twenty seconds: it defers to c
            # afresh instead of resuming its count from +10.5.
            assert [run.replica for run in fleet.runs] == ["a", "b", "c"]

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_slow_handoff_check_does_not_hold_back_a_renewal(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def slow_once_leading(_ctx: Any) -> bool:
                if fleet.runs:  # a dependency degrades once a holds the lease
                    await asyncio.sleep(100)

                return False

            a = fleet.replica("a", handoff="on_request", prefer=slow_once_leading)
            await a.start()
            await asyncio.sleep(5)
            await fleet.replica("b", handoff="on_request", prefer=self._prefers(True)).start()
            await asyncio.sleep(40)

            first = fleet.runs[0]
            assert first.term.live()  # renewed every ten seconds, check or no check
            assert first.term.reason is None

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_late_yield_is_cut_short_of_the_hard_deadline(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            spec = attrs.evolve(SPEC, renew_every=timedelta(seconds=1))

            async def slow_once_leading(_ctx: Any) -> bool:
                if fleet.runs:
                    await asyncio.sleep(1.0)

                return False

            a = fleet.replica(
                "a",
                spec=spec,
                handoff="on_request",
                prefer=slow_once_leading,
                lead=fleet.body("a", listens=False, resists=0.5),
            )
            # The renewal sent at +1 answers at +23.5, moving the hard deadline to +30; the
            # next ones hang. The handoff check then takes a second, so a yields at +24.5.
            a.lock.reset_plan = [22.5] + [None] * 20
            await a.start()
            await asyncio.sleep(20)
            await fleet.replica("b", spec=spec, handoff="on_request", prefer=self._prefers(True)).start()
            await asyncio.sleep(20)

            yielded = fleet.runs[0]
            assert yielded.term.reason == "yielded"
            hard = 1.0 + TTL - MARGIN
            assert yielded.cancelled == hard - RELEASE  # not +29.5: the grace is cut
            assert yielded.finished is not None and yielded.finished <= hard

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_dead_asker_takes_the_unit_at_most_once(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            await fleet.replica("a", handoff="on_request", prefer=self._prefers(False)).start()
            await asyncio.sleep(5)
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            await b.start()
            await asyncio.sleep(4.5)
            b.kill()  # dies holding the want-lock
            await asyncio.sleep(60)

            yields = [run for run in fleet.runs if run.term.reason == "yielded"]
            assert len(yields) <= 1, [(r.started, r.term.reason) for r in fleet.runs]

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_want_lock_landing_after_its_asker_stopped_is_released(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            await fleet.replica("a", handoff="on_request", prefer=self._prefers(False)).start()
            await asyncio.sleep(1)
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            b.lock.want_delay = 5.0
            await b.start()
            await asyncio.sleep(0.5)
            b.kill()  # the want-lock request is on the wire
            await asyncio.sleep(40)

            sent, owner = b.lock.calls("acquire", f"{KEY}:want")[0]
            assert (sent + 5.0, owner) in b.lock.calls("release", f"{KEY}:want")
            assert [run.replica for run in fleet.runs] == ["a"]  # a never yields to it
            assert fleet.runs[0].term.live()

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_failed_handoff_check_keeps_the_term(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", handoff="on_request", prefer=self._prefers(False))
            await a.start()
            await asyncio.sleep(1)
            a.lock.reads_fail = True  # the holder can no longer read the want-lock
            await asyncio.sleep(4)
            await fleet.replica("b", handoff="on_request", prefer=self._prefers(True)).start()
            await asyncio.sleep(30)

            (run,) = fleet.runs
            assert run.replica == "a" and run.term.live()  # renewed, and did not yield
            assert len(a.lock.calls("reset")) == 3

            await fleet.stop_all()

        _simulate(scenario)

    def test_two_preferring_replicas_do_not_pass_the_unit_back_and_forth(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            await fleet.replica("a", handoff="on_request", prefer=self._prefers(True)).start()
            await asyncio.sleep(5)
            await fleet.replica("b", handoff="on_request", prefer=self._prefers(True)).start()
            await asyncio.sleep(100)

            assert [run.replica for run in fleet.runs] == ["a"]
            assert fleet.runs[0].term.live()

            await fleet.stop_all()

        _simulate(scenario)

    def test_an_asker_that_may_not_campaign_never_takes_the_want_lock(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def refuse(_ctx: Any) -> bool:
                return False

            await fleet.replica("a", handoff="on_request", prefer=self._prefers(False)).start()
            b = fleet.replica(
                "b", handoff="on_request", prefer=self._prefers(True), may_campaign=refuse
            )
            await b.start()
            await asyncio.sleep(40)

            assert not b.lock.log
            assert [run.replica for run in fleet.runs] == ["a"]

            await fleet.stop_all()

        _simulate(scenario)

    def test_an_asker_that_cannot_acquire_gives_up_and_the_fleet_still_elects(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            b.lock.acquire_fails_on = frozenset({KEY})
            await b.start()
            start = monotonic()
            await fleet.replica("a", handoff="on_request", prefer=self._prefers(False)).start()
            await fleet.replica("c", handoff="on_request", prefer=self._prefers(False)).start()
            await asyncio.sleep(30)

            third = b.lock.calls("acquire")[2][0]  # every lease acquire of b's raises
            asks = [at for at, _ in b.lock.calls("acquire", f"{KEY}:want")]
            # Each failed acquire retires its string, and the want-lock moves to the next one;
            # the third gives the want-lock up, and b does not ask again for a ttl.
            assert third in [at for at, _ in b.lock.calls("release", f"{KEY}:want")]
            assert asks[:3] == [at for at, _ in b.lock.calls("acquire")][:3]
            assert not [at for at in asks if third < at < third + TTL]
            (leader,) = fleet.runs
            assert leader.replica in {"a", "c"}
            assert leader.started - start <= (3 + 1) * CAMPAIGN

            await fleet.stop_all()

        _simulate(scenario)


# ....................... #


class TestCampaignFailures:
    """Handoff is a courtesy: a replica that cannot read or keep it campaigns without it."""

    def _prefers(self, value: bool) -> Callable[[Any], Awaitable[bool]]:
        async def prefer(_ctx: Any) -> bool:
            return value

        return prefer

    def test_failing_want_lock_reads_do_not_leave_the_lease_unled(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            for name in ("a", "b", "c"):
                r = fleet.replica(name, handoff="on_request", prefer=self._prefers(False))
                r.lock.reads_fail = True
                await r.start()

            await asyncio.sleep(10)

            assert len(fleet.runs) == 1
            assert fleet.runs[0].started == 0.0

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_failing_prefer_does_not_leave_the_lease_unled(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def broken(_ctx: Any) -> bool:
                raise exc.infrastructure("the preference service is down")

            for name in ("a", "b"):
                await fleet.replica(name, handoff="on_request", prefer=broken).start()

            await asyncio.sleep(10)

            assert len(fleet.runs) == 1
            assert fleet.runs[0].started == 0.0

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_survivor_whose_reads_fail_still_takes_over(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", handoff="on_request", prefer=self._prefers(False))
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(False))
            await a.start()
            await asyncio.sleep(1)
            await b.start()
            await asyncio.sleep(5)

            b.lock.reads_fail = True
            a.kill()
            await asyncio.sleep(TTL + 2 * CAMPAIGN)

            assert [run.replica for run in fleet.runs] == ["a", "b"]

            await fleet.stop_all()

        _simulate(scenario)

    def test_slow_campaign_reads_do_not_leave_the_lease_unled(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            for name in ("a", "b"):
                r = fleet.replica(name, handoff="on_request", prefer=self._prefers(False))
                r.lock.read_delay = 1.5  # past release_timeout, inside a campaign interval
                await r.start()

            await asyncio.sleep(10)

            assert len(fleet.runs) == 1

            await fleet.stop_all()

        _simulate(scenario)

    def test_an_asker_counts_only_consecutive_failed_acquires(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            await fleet.replica("a", handoff="on_request", prefer=self._prefers(True)).start()
            await asyncio.sleep(1)
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            b.lock.acquire_fails_at = frozenset({2, 10, 20})  # three, never in a row
            await b.start()
            await asyncio.sleep(60)

            third = b.lock.calls("acquire")[19][0]
            asks = [at for at, _ in b.lock.calls("acquire", f"{KEY}:want")]
            # b keeps asking after the third: no cool-off, which three failures in a row cause.
            assert [at for at in asks if third < at < third + 2 * CAMPAIGN + 0.5]

            await fleet.stop_all()

        _simulate(scenario)

    def test_an_asker_whose_acquires_hang_is_yielded_to_at_most_once_a_ttl(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            for name in ("a", "c"):
                await fleet.replica(name, handoff="on_request", prefer=self._prefers(False)).start()

            await asyncio.sleep(9)
            b = fleet.replica("b", handoff="on_request", prefer=self._prefers(True))
            b.lock.acquire_delay = 100_000.0  # every lease acquire hangs past its wait
            await b.start()
            await asyncio.sleep(300)

            yields = [run.saw_end for run in fleet.runs if run.term.reason == "yielded"]
            assert yields
            assert all(
                later - earlier >= TTL for earlier, later in zip(yields, yields[1:])  # type: ignore[operator]
            ), yields

            await fleet.stop_all()

        _simulate(scenario)


class TestWiring:
    def test_a_second_startup_while_running_is_ignored(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a")
            await a.start()
            first = a.loop_task()
            await a.start()

            assert a.loop_task() is first
            assert [loop.loop_name for loop in a.ctx.drainables.loops] == ["leadership:relay"]

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_configuration_error_in_a_campaign_hook_stops_campaigning(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def broken(_ctx: Any) -> bool:
                raise exc.configuration("misconfigured")

            a = fleet.replica("a", may_campaign=broken)
            await a.start()
            await asyncio.sleep(10)

            assert a.loop_task().done()
            assert not a.lock.log

        _simulate(scenario)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"key": ""},
            {"margin": timedelta(seconds=-1)},
            {"yield_grace": timedelta(seconds=-1)},
            {"campaign_every": timedelta(0)},
            {"release_timeout": timedelta(0)},
            {"renew_every": timedelta(0)},
        ],
    )
    def test_the_spec_refuses_nonsense(self, kwargs: dict[str, Any]) -> None:
        with pytest.raises(CoreException) as ei:
            LeadershipSpec(lock=LOCK, **{"key": KEY, **kwargs})

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_a_lease_key_may_not_look_like_a_want_lock(self) -> None:
        with pytest.raises(CoreException) as ei:
            LeadershipSpec(lock=LOCK, key="relay:want")

        assert ei.value.code == "leadership.key_reserved"
        LeadershipSpec(lock=LOCK, key="relay:wanted")

    def test_the_spec_refuses_a_grace_that_does_not_fit_the_lease(self) -> None:
        # 23 + 5 + 1 + 1 = 30: the grace fits, but the unwind after it would not.
        with pytest.raises(CoreException) as ei:
            LeadershipSpec(lock=LOCK, key=KEY, renew_every=timedelta(seconds=23))

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        LeadershipSpec(lock=LOCK, key=KEY, renew_every=timedelta(seconds=22))

        # A cut before the soft deadline would cancel a body before it is told.
        with pytest.raises(CoreException):
            LeadershipSpec(
                lock=LOCK,
                key=KEY,
                yield_grace=timedelta(seconds=1),
                release_timeout=timedelta(seconds=2),
            )

        with pytest.raises(CoreException):
            LeadershipSpec(lock=LOCK, key=KEY, campaign_every=timedelta(seconds=30))

        assert SPEC.renew_interval == timedelta(seconds=10)

    @pytest.mark.parametrize(
        ("kwargs", "code"),
        [
            ({"fence_floor": object()}, "leadership.fence_floor_without_advance"),
            ({"handoff": "on_request"}, None),
            ({"handoff": "always"}, None),
            ({"owner": ""}, None),
        ],
    )
    def test_the_step_refuses_what_it_cannot_honour(
        self, kwargs: dict[str, Any], code: str | None
    ) -> None:
        async def lead(_ctx: Any, _term: Term) -> None:
            return None

        with pytest.raises(CoreException) as ei:
            leadership_lifecycle_step(SPEC, lead=lead, **{"owner": "pod", **kwargs})

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert code is None or ei.value.code == code

    def test_a_claim_needs_a_fencing_lock(self) -> None:
        async def lead(_ctx: Any, _term: Term) -> None:
            return None

        async def claim(_ctx: Any, _term: Term) -> bool:
            return True

        unfenced = LeadershipSpec(lock=DistributedLockSpec(name="leader"), key=KEY)

        with pytest.raises(CoreException) as ei:
            leadership_lifecycle_step(unfenced, owner="pod", lead=lead, claim=claim)

        assert ei.value.code == "leadership.claim_without_fencing"

    def test_fleet_accepts_the_step_unguarded_and_serverless_refuses_it(self) -> None:
        async def lead(_ctx: Any, _term: Term) -> None:
            return None

        step = leadership_lifecycle_step(SPEC, owner="pod", lead=lead)
        plan = LifecyclePlan.from_steps(step).freeze()

        assert step.requires_long_running
        assert not step.mutates_shared_state and not step.singleton_guarded
        ExecutionRuntime(lifecycle=plan, deployment=DeploymentProfile.FLEET)

        with pytest.raises(CoreException) as ei:
            ExecutionRuntime(lifecycle=plan, deployment=DeploymentProfile.SERVERLESS)

        assert "leadership:relay" in ei.value.summary

    def test_two_runtimes_elect_one_leader_and_scope_exit_releases(self) -> None:
        async def scenario() -> None:
            state = MockState()
            leading: list[str] = []

            def runtime(name: str) -> ExecutionRuntime:
                async def lead(_ctx: Any, term: Term) -> None:
                    leading.append(name)
                    await term.ended.wait()

                step = leadership_lifecycle_step(SPEC, owner=name, lead=lead)

                return ExecutionRuntime(
                    deps=_deps(state),
                    lifecycle=LifecyclePlan.from_steps(step).freeze(),
                    deployment=DeploymentProfile.FLEET,
                )

            first, second = runtime("a"), runtime("b")

            async with first.scope():
                async with second.scope():
                    await asyncio.sleep(10)
                    assert leading == ["a"]

                lock = first.get_context().dlock.query(LOCK)
                assert await lock.get_owner(KEY) is not None

            assert await MockDistributedLockAdapter(
                spec=LOCK, state=state, namespace="leader"
            ).is_locked(KEY) is False

        _simulate(scenario)


def _deps(state: MockState) -> Any:
    from forze.application.execution import DepsRegistry

    return DepsRegistry.from_modules(MockDepsModule(state=state)).freeze()


# ....................... #


class TestFenceFloor:
    """After the lock backend loses its counter, ``fence_floor`` moves it past the store's."""

    # Only the advance flag: it requires fencing tokens too.
    ADVANCING = attrs.evolve(SPEC, lock=DistributedLockSpec(name="leader", requires_fence_advance=True))

    def _claims(self, store: dict[str, int], log: list[tuple[float, int, bool]]) -> Any:
        async def claim(_ctx: Any, term: Term) -> bool:
            assert term.token is not None
            ok = store["fence"] < term.token
            store["fence"] = max(store["fence"], term.token)
            log.append((monotonic(), term.token, ok))
            return ok

        return claim

    def test_the_first_term_after_a_loss_claims_at_once(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            store = {"fence": 50}  # the store saw tokens up to 50; the lock backend is empty
            claims: list[tuple[float, int, bool]] = []

            async def floor(_ctx: Any) -> int:
                return store["fence"]

            for name in ("a", "b"):
                await fleet.replica(
                    name,
                    spec=self.ADVANCING,
                    claim=self._claims(store, claims),
                    fence_floor=floor,
                ).start()

            await asyncio.sleep(30)

            assert claims == [(0.0, 51, True)]
            assert [run.term.token for run in fleet.runs] == [51]

            await fleet.stop_all()

        _simulate(scenario)

    def test_without_it_the_fleet_stalls_until_the_counter_catches_up(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            store = {"fence": 50}
            claims: list[tuple[float, int, bool]] = []

            for name in ("a", "b"):
                await fleet.replica(name, claim=self._claims(store, claims)).start()

            await asyncio.sleep(200)

            first = next(at for at, _, ok in claims if ok)
            assert [token for _, token, ok in claims if ok][:1] == [51]
            assert first >= 50 * CAMPAIGN * 0.9  # a token per campaign interval, roughly

            await fleet.stop_all()

        _simulate(scenario)

    def test_it_advances_only_while_the_lease_is_free(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            reads: list[float] = []

            async def floor(_ctx: Any) -> int:
                reads.append(monotonic())
                return 0

            await fleet.replica("a", spec=self.ADVANCING, fence_floor=floor).start()
            await asyncio.sleep(1)
            await fleet.replica("b", spec=self.ADVANCING, fence_floor=floor).start()
            await asyncio.sleep(30)

            assert reads == [0.0]  # a's first campaign; b found the lease held every time

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_failing_floor_does_not_hold_up_the_campaign(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def floor(_ctx: Any) -> int:
                raise exc.infrastructure("the store is unreachable")

            a = fleet.replica("a", spec=self.ADVANCING, fence_floor=floor)
            await a.start()
            await asyncio.sleep(10)

            # Without the floor the token may be low; the claim and the fenced writes refuse
            # it then. A free lease left unled helps nobody.
            (run,) = fleet.runs
            assert run.started == 0.0
            assert not a.lock.calls("advance", KEY)

            await fleet.stop_all()

        _simulate(scenario)

    def test_a_slow_floor_read_does_not_hold_up_the_campaign(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def floor(_ctx: Any) -> int:
                await asyncio.sleep(1.5)  # a store query under load, past release_timeout
                return 7

            for name in ("a", "b"):
                await fleet.replica(name, spec=self.ADVANCING, fence_floor=floor).start()

            await asyncio.sleep(30)

            assert [run.term.token for run in fleet.runs] == [8]  # advanced, then acquired

            await fleet.stop_all()

        _simulate(scenario)

    def test_an_exhausted_counter_stops_campaigning(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def floor(_ctx: Any) -> int:
                return FENCE_MAX_VALUE

            a = fleet.replica("a", spec=self.ADVANCING, fence_floor=floor)
            await a.start()
            await asyncio.sleep(20)

            assert a.loop_task().done()
            assert len(a.lock.calls("acquire")) == 1  # refused once, not every interval
            assert not fleet.runs

        _simulate(scenario)

    def test_a_floor_the_lock_cannot_advance_past_stops_campaigning(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def floor(_ctx: Any) -> Any:
                return None  # no row in the store yet, returned as is

            a = fleet.replica("a", spec=self.ADVANCING, fence_floor=floor)
            await a.start()
            await asyncio.sleep(20)

            assert a.loop_task().done()
            assert not a.lock.calls("acquire")

        _simulate(scenario)

    def test_a_configuration_error_in_the_floor_stops_campaigning(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()

            async def floor(_ctx: Any) -> int:
                raise exc.configuration("no such table")

            a = fleet.replica("a", spec=self.ADVANCING, fence_floor=floor)
            await a.start()
            await asyncio.sleep(10)

            assert a.loop_task().done()
            assert not a.lock.calls("acquire")

        _simulate(scenario)

    def test_it_needs_the_capability_on_the_spec(self) -> None:
        async def lead(_ctx: Any, _term: Term) -> None:
            return None

        async def floor(_ctx: Any) -> int:
            return 0

        with pytest.raises(CoreException) as ei:
            leadership_lifecycle_step(SPEC, owner="pod", lead=lead, fence_floor=floor)

        assert ei.value.code == "leadership.fence_floor_without_advance"

    def test_a_backend_without_the_capability_fails_at_startup(self) -> None:
        async def scenario() -> None:
            class _FencingOnly:
                """Issues fencing tokens and cannot advance a counter."""

                def capabilities(self) -> DistributedLockCapabilities:
                    return DistributedLockCapabilities(fencing_tokens=True)

            async def lead(_ctx: Any, _term: Term) -> None:
                return None

            async def floor(_ctx: Any) -> int:
                return 0

            step = leadership_lifecycle_step(
                self.ADVANCING, owner="pod", lead=lead, fence_floor=floor
            )
            ctx = context_from_deps(
                Deps.plain(
                    {
                        DistributedLockCommandDepKey: lambda _c, _s: _FencingOnly(),
                        DistributedLockQueryDepKey: lambda _c, _s: _FencingOnly(),
                    }
                )
            )

            with pytest.raises(CoreException) as ei:
                await step.startup(ctx)

            assert ei.value.code == "dlock.fence_advance_unsupported"

        _simulate(scenario)

    def test_a_claim_is_accepted_with_either_fencing_flag(self) -> None:
        async def lead(_ctx: Any, _term: Term) -> None:
            return None

        async def claim(_ctx: Any, _term: Term) -> bool:
            return True

        leadership_lifecycle_step(self.ADVANCING, owner="pod", lead=lead, claim=claim)


class TestMetrics:
    def test_held_and_terms_ended(self) -> None:
        reader = InMemoryMetricReader()
        instrument_leadership(meter=MeterProvider(metric_readers=[reader]).get_meter("test"))
        spec = attrs.evolve(SPEC, key="metrics-unit")

        def points(name: str) -> dict[tuple[tuple[str, Any], ...], Any]:
            out: dict[tuple[tuple[str, Any], ...], Any] = {}

            for rm in reader.get_metrics_data().resource_metrics:
                for sm in rm.scope_metrics:
                    for metric in sm.metrics:
                        if metric.name == name:
                            for dp in metric.data.data_points:
                                attrs_ = dict(dp.attributes)

                                if attrs_.get("forze.leadership.key") == "metrics-unit":
                                    out[tuple(sorted(attrs_.items()))] = dp.value

            return out

        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", spec=spec)
            await a.start()
            await asyncio.sleep(1)

            assert list(points("forze.leadership.held").values()) == [1]

            await a.stop()

            assert list(points("forze.leadership.held").values()) == [0]
            ended = points("forze.leadership.terms_ended")
            assert ended == {
                (("forze.leadership.key", "metrics-unit"), ("forze.leadership.reason", "stopped")): 1
            }

        _simulate(scenario)
