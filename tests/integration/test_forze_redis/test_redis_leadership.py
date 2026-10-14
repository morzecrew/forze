"""Lifetime leadership against a real Redis: election, takeover, a deleted lease, shared owners.

Real time, so the lease is short (1.5 s) and the bounds carry a little slack for scheduling.
Each replica is its own step, context and adapter over one Redis namespace.
"""

from __future__ import annotations

import asyncio
import math
from datetime import timedelta
from typing import Any
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.execution.context.drainable import Drainables
from forze.base.primitives import monotonic
from forze_kits.lifecycle import LeadershipSpec, Term, leadership_lifecycle_step
from forze_redis.adapters import RedisDistributedLockAdapter
from forze_redis.kernel.client import RedisClient

pytestmark = pytest.mark.asyncio

# ----------------------- #

KEY = "relay"
TTL, RENEW, CAMPAIGN, MARGIN = 1.5, 0.3, 0.2, 0.1
SLACK = 0.5
"""Room for real scheduling and round trips on top of a bound."""

SPEC = LeadershipSpec(
    lock=DistributedLockSpec(
        name="it-leader",
        ttl=timedelta(seconds=TTL),
        requires_fencing_token=True,
    ),
    key=KEY,
    renew_every=timedelta(seconds=RENEW),
    campaign_every=timedelta(seconds=CAMPAIGN),
    margin=timedelta(seconds=MARGIN),
    yield_grace=timedelta(seconds=0.3),
    release_timeout=timedelta(seconds=0.3),
)


@attrs.define(slots=True, eq=False)
class _Recorded:
    """The Redis lock, noting when each renewal was sent."""

    inner: RedisDistributedLockAdapter
    resets: list[float] = attrs.field(factory=list)

    @property
    def spec(self) -> DistributedLockSpec:
        return self.inner.spec

    async def acquire(self, key: str, owner: str) -> Any:
        return await self.inner.acquire(key, owner)

    async def reset(self, key: str, owner: str) -> bool:
        if key == KEY:
            self.resets.append(monotonic())

        return await self.inner.reset(key, owner)

    async def release(self, key: str, owner: str) -> bool:
        return await self.inner.release(key, owner)

    async def is_locked(self, key: str) -> bool:
        return await self.inner.is_locked(key)

    async def get_owner(self, key: str) -> str | None:
        return await self.inner.get_owner(key)

    async def get_ttl(self, key: str) -> timedelta | None:
        return await self.inner.get_ttl(key)


@attrs.define(slots=True, eq=False)
class _Dlock:
    lock: RedisDistributedLockAdapter
    recorded: _Recorded = attrs.field(
        default=attrs.Factory(lambda self: _Recorded(inner=self.lock), takes_self=True)
    )

    def command(self, _spec: DistributedLockSpec) -> _Recorded:
        return self.recorded

    def query(self, _spec: DistributedLockSpec) -> _Recorded:
        return self.recorded


@attrs.define(slots=True, eq=False)
class _Ctx:
    dlock: _Dlock
    drainables: Drainables = attrs.field(factory=Drainables)


@attrs.define(slots=True, eq=False)
class _Run:
    replica: str
    term: Term
    started: float
    finished: float | None = None
    cancelled: float | None = None


@attrs.define(slots=True, eq=False)
class _Fleet:
    client: RedisClient
    namespace: str = attrs.field(factory=lambda: f"it:leader:{uuid4().hex[:12]}")
    runs: list[_Run] = attrs.field(factory=list)
    replicas: list[tuple[Any, _Ctx]] = attrs.field(factory=list)

    def lock(self) -> RedisDistributedLockAdapter:
        return RedisDistributedLockAdapter(
            client=self.client,
            namespace=self.namespace,
            spec=SPEC.lock,
        )

    async def start(
        self,
        name: str,
        *,
        owner: str | None = None,
        **step_kw: Any,
    ) -> tuple[Any, _Ctx]:
        async def lead(_ctx: Any, term: Term) -> None:
            run = _Run(replica=name, term=term, started=monotonic())
            self.runs.append(run)

            try:
                await term.ended.wait()

            except asyncio.CancelledError:
                run.cancelled = monotonic()
                raise

            finally:
                run.finished = monotonic()

        step = leadership_lifecycle_step(SPEC, owner=owner or name, lead=lead, **step_kw)
        ctx = _Ctx(dlock=_Dlock(lock=self.lock()))
        await step.startup(ctx)  # type: ignore[arg-type]
        self.replicas.append((step, ctx))

        return step, ctx

    async def stop_all(self) -> None:
        await asyncio.gather(*(ctx.drainables.stop_all(grace=5) for _, ctx in self.replicas))

    def assert_one_at_a_time(self) -> None:
        spans = sorted(
            (run.started, run.finished if run.finished is not None else math.inf)
            for run in self.runs
        )

        for (_, first_end), (second_start, _) in zip(spans, spans[1:]):
            assert second_start >= first_end, spans


def _kill(step: Any) -> None:
    """The process dies: its loop and body are cancelled, and nothing is released."""

    step.startup.control.task.cancel()


async def _wait_for(predicate: Any, *, within: float) -> None:
    deadline = monotonic() + within

    while not predicate():
        if monotonic() >= deadline:
            raise AssertionError(f"not reached within {within}s")

        await asyncio.sleep(0.02)


# ----------------------- #


async def test_one_leader_among_three(redis_client: RedisClient) -> None:
    fleet = _Fleet(client=redis_client)

    try:
        for name in ("a", "b", "c"):
            await fleet.start(name)

        await asyncio.sleep(3 * TTL)  # many renewals

        assert len(fleet.runs) == 1
        assert fleet.runs[0].term.live()
        assert fleet.runs[0].term.token is not None

    finally:
        await fleet.stop_all()

    assert fleet.runs[0].term.reason == "stopped"


async def test_takeover_after_a_kill_within_ttl_plus_a_campaign_interval(
    redis_client: RedisClient,
) -> None:
    fleet = _Fleet(client=redis_client)

    try:
        first, first_ctx = await fleet.start("a")
        await _wait_for(lambda: len(fleet.runs) == 1, within=1)
        await fleet.start("b")
        await asyncio.sleep(1)

        killed_at = monotonic()
        _kill(first)
        await _wait_for(lambda: len(fleet.runs) == 2, within=TTL + CAMPAIGN + SLACK)

        old, new = fleet.runs
        assert new.replica == "b"
        assert new.started - killed_at <= TTL + CAMPAIGN + SLACK
        # Redis held the lease for a ttl from the last renewal the dead leader sent.
        last_renewal = max(first_ctx.dlock.recorded.resets)
        assert new.started >= last_renewal + TTL - MARGIN
        assert new.term.token is not None and old.term.token is not None
        assert new.term.token > old.term.token
        fleet.assert_one_at_a_time()

    finally:
        await fleet.stop_all()


async def test_a_deleted_lease_is_lost_at_the_next_renewal(redis_client: RedisClient) -> None:
    fleet = _Fleet(client=redis_client)

    try:
        _, ctx = await fleet.start("a")
        await _wait_for(lambda: len(fleet.runs) == 1, within=1)

        lock = ctx.dlock.lock
        deleted_at = monotonic()
        assert await redis_client.delete(lock.construct_key("dlock", KEY)) == 1

        (run,) = fleet.runs
        await _wait_for(lambda: run.finished is not None, within=RENEW + SLACK)

        assert run.term.reason == "lost"
        assert run.cancelled is not None  # cancelled, not told
        assert run.cancelled - deleted_at <= RENEW + SLACK

    finally:
        await fleet.stop_all()


async def test_replicas_sharing_an_owner_never_both_lead(redis_client: RedisClient) -> None:
    fleet = _Fleet(client=redis_client)

    try:
        first, ctx = await fleet.start("a", owner="pod")
        await _wait_for(lambda: len(fleet.runs) == 1, within=1)
        await fleet.start("b", owner="pod")
        await asyncio.sleep(1)

        assert len(fleet.runs) == 1

        _kill(first)
        await _wait_for(lambda: len(fleet.runs) == 2, within=TTL + CAMPAIGN + SLACK)

        old, new = fleet.runs
        assert new.started >= max(ctx.dlock.recorded.resets) + TTL - MARGIN
        assert old.term.owner != new.term.owner
        # The old term's late release cannot delete the successor's lease.
        lock = ctx.dlock.lock
        assert await lock.release(KEY, old.term.owner) is False
        assert await lock.get_owner(KEY) == new.term.owner
        fleet.assert_one_at_a_time()

    finally:
        await fleet.stop_all()


async def test_a_preferring_replica_takes_the_unit_at_the_next_renewal(
    redis_client: RedisClient,
) -> None:
    fleet = _Fleet(client=redis_client)

    def prefers(value: bool) -> Any:
        async def prefer(_ctx: Any) -> bool:
            return value

        return prefer

    try:
        await fleet.start("a", handoff="on_request", prefer=prefers(False))
        await _wait_for(lambda: len(fleet.runs) == 1, within=1)
        await fleet.start("b", handoff="on_request", prefer=prefers(True))
        await _wait_for(lambda: len(fleet.runs) == 2, within=RENEW + CAMPAIGN + SLACK)

        holder, asker = fleet.runs
        assert holder.term.reason == "yielded"
        assert asker.replica == "b"
        fleet.assert_one_at_a_time()

        await asyncio.sleep(3 * RENEW)
        assert len(fleet.runs) == 2  # a preferring holder keeps it

    finally:
        await fleet.stop_all()
