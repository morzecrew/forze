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

from forze.application.contracts.dlock import AcquiredLock, DistributedLockSpec
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import DeploymentProfile
from forze.application.execution.context.drainable import Drainables
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import monotonic
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
    acquire_fails_on: frozenset[str] = frozenset()
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

        if self.acquire_delay:
            await asyncio.sleep(self.acquire_delay)

        return await self.inner.acquire(key, owner)

    async def reset(self, key: str, owner: str) -> bool:
        self.log.append((monotonic(), "reset", key, owner))

        if key == KEY and self.reset_plan:
            delay = self.reset_plan.pop(0)
            await asyncio.sleep(FOREVER if delay is None else delay)

        return await self.inner.reset(key, owner)

    async def release(self, key: str, owner: str) -> bool:
        self.log.append((monotonic(), "release", key, owner))

        return await self.inner.release(key, owner)

    async def is_locked(self, key: str) -> bool:
        return await self.inner.is_locked(key)

    async def get_owner(self, key: str) -> str | None:
        return await self.inner.get_owner(key)

    async def get_ttl(self, key: str) -> timedelta | None:
        return await self.inner.get_ttl(key)


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

    def test_a_body_that_ignores_its_end_is_cancelled_at_the_hard_deadline(self) -> None:
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
            assert leader.cancelled == sent + TTL - MARGIN
            assert successor.started >= leader.cancelled
            fleet.assert_one_at_a_time()

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

    def test_a_replica_does_not_lead_beside_a_body_it_abandoned(self) -> None:
        async def scenario() -> None:
            fleet = _Fleet()
            a = fleet.replica("a", lead=fleet.body("a", listens=False, resists=100))
            a.lock.reset_plan = [None] * 10
            await a.start()
            await asyncio.sleep(200)

            first, second = fleet.runs
            assert first.cancelled == _first_acquire(a) + TTL - MARGIN
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

            failed = b.lock.calls("acquire")
            gave_up = b.lock.calls("release", f"{KEY}:want")
            assert gave_up and gave_up[0][0] == failed[2][0]  # at the third failed campaign
            (leader,) = fleet.runs
            assert leader.replica in {"a", "c"}
            assert leader.started - start <= (3 + 1) * CAMPAIGN

            await fleet.stop_all()

        _simulate(scenario)


# ....................... #


class TestWiring:
    def test_the_spec_refuses_a_grace_that_does_not_fit_the_lease(self) -> None:
        with pytest.raises(CoreException) as ei:
            LeadershipSpec(lock=LOCK, key=KEY, renew_every=timedelta(seconds=24))

        assert ei.value.kind is ExceptionKind.CONFIGURATION

        with pytest.raises(CoreException):
            LeadershipSpec(lock=LOCK, key=KEY, campaign_every=timedelta(seconds=30))

        assert SPEC.renew_interval == timedelta(seconds=10)

    @pytest.mark.parametrize(
        ("kwargs", "code"),
        [
            ({"fence_floor": object()}, "leadership.fence_floor_unsupported"),
            ({"handoff": "on_request"}, None),
        ],
    )
    def test_the_step_refuses_what_it_cannot_honour(
        self, kwargs: dict[str, Any], code: str | None
    ) -> None:
        async def lead(_ctx: Any, _term: Term) -> None:
            return None

        with pytest.raises(CoreException) as ei:
            leadership_lifecycle_step(SPEC, owner="pod", lead=lead, **kwargs)

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
