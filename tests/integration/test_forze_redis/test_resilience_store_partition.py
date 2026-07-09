"""Resilience stores under partition — the divergences a distributed store adds over the local one.

The in-memory stores are the reference; the Redis stores share state across replicas so the fleet
trips/limits together. This file pins what a real Redis store does when the network splits — the paths
the in-memory model can't exhibit and that the happy-path Lua tests (`test_redis_*`) don't reach:

* **Fleet-rate collapse to per-replica** — a shared limiter enforces one fleet budget; the instant a
  replica loses Redis it fails open to its *own* in-memory bucket, so the fleet-effective rate jumps
  back to ``permits × replicas``.
* **Local fast-path cache staleness** — the breaker's ``local_cache_ttl`` (0.25s) closed-phase cache
  serves ``admit`` without a Redis read, so a fleet trip elsewhere is not seen until the window lapses.

A partition is simulated by a client wrapper whose ``run_script`` raises — the stores catch any Redis
error and fall open to a process-local fallback (that is the whole point: the store must never become
a per-call single point of failure). Both stores expose an injectable clock, so the cache window is
driven deterministically with no sleeping.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Sequence, cast
from uuid import uuid4

import attrs

from forze.application.contracts.resilience import (
    CircuitBreakerStrategy,
    RateLimitStrategy,
)
from forze_redis.adapters.circuit_breaker import RedisCircuitBreakerStore
from forze_redis.adapters.rate_limit import RedisRateLimitStore
from forze_redis.kernel.client import RedisClient, RedisClientPort

# ----------------------- #

_KEY = ("p", "r")


@attrs.define
class _PartitionableClient:
    """Wraps a real Redis client; when ``partitioned`` its ``run_script`` raises, modeling the store
    losing Redis. The resilience stores only call ``run_script``, so nothing else needs delegating."""

    inner: RedisClient
    partitioned: bool = False

    async def run_script(
        self, script: str, keys: Sequence[str], args: Sequence[Any]
    ) -> str:
        if self.partitioned:
            raise ConnectionError("simulated Redis partition")

        return await self.inner.run_script(script, keys, args)


class _Clock:
    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


# ....................... #


class TestFleetRateCollapseUnderPartition:
    async def test_partitioned_replica_regains_a_per_replica_budget(
        self, redis_client: RedisClient
    ) -> None:
        namespace = f"it:ratelimit:{uuid4().hex[:12]}"
        # per = 1h so refill over the test's few ms is negligible: capacity 3 = exactly 3 admits.
        strat = RateLimitStrategy(permits=3, per=timedelta(hours=1))

        a = RedisRateLimitStore(client=redis_client, namespace=namespace)
        link = _PartitionableClient(inner=redis_client)
        b = RedisRateLimitStore(
            client=cast(RedisClientPort, link), namespace=namespace
        )

        # Fleet-shared: replicas A and B draw from ONE Redis bucket of 3 — three admits total, then
        # both are denied. This is the guarantee the shared store exists to provide.
        assert await a.try_acquire(_KEY, strat) is True
        assert await b.try_acquire(_KEY, strat) is True
        assert await a.try_acquire(_KEY, strat) is True
        assert await a.try_acquire(_KEY, strat) is False
        assert await b.try_acquire(_KEY, strat) is False

        # Partition B: it falls open to its own in-memory bucket — a FRESH capacity of 3 the fleet
        # limit should have denied. The fleet rate has collapsed to per-replica for the split node.
        link.partitioned = True
        local_admits = sum(
            [await b.try_acquire(_KEY, strat) for _ in range(5)]
        )
        assert local_admits == 3

        # A is still connected and still sees the (empty) shared bucket — unaffected by B's split.
        assert await a.try_acquire(_KEY, strat) is False


class TestBreakerFastPathCacheStaleness:
    async def test_closed_cache_hides_a_fleet_trip_until_the_ttl_lapses(
        self, redis_client: RedisClient
    ) -> None:
        namespace = f"it:breaker:{uuid4().hex[:12]}"
        # break_duration long so the trip stays OPEN on the server clock for the whole test (the
        # injected clock only drives B's local cache, never the Lua's server-time phase).
        strat = CircuitBreakerStrategy(
            failure_ratio=0.5,
            sampling_window=timedelta(seconds=10),
            min_throughput=2,
            break_duration=timedelta(seconds=60),
            half_open_max_calls=1,
        )
        clock = _Clock()
        tripper = RedisCircuitBreakerStore(
            client=redis_client, namespace=namespace, local_cache_ttl=0.25, clock=clock
        )
        replica = RedisCircuitBreakerStore(
            client=redis_client, namespace=namespace, local_cache_ttl=0.25, clock=clock
        )

        # The replica warms its closed fast-path cache (this admit reads Redis, caches "closed").
        allowed, _ = await replica.admit(_KEY, strat)
        assert allowed is True

        # Another replica trips the SHARED breaker — Redis now holds the open phase.
        assert await tripper.record(_KEY, strat, False) is None
        assert await tripper.record(_KEY, strat, False) == "open"

        # Within the 0.25s cache window (clock unmoved) the replica still admits: the stale closed
        # fast-path never reads Redis, so it has not seen the fleet trip.
        allowed, tr = await replica.admit(_KEY, strat)
        assert allowed is True
        assert tr is None

        # Past the TTL the fast-path expires; the next admit reads Redis and sees the open state.
        clock.advance(0.26)
        allowed, _ = await replica.admit(_KEY, strat)
        assert allowed is False
