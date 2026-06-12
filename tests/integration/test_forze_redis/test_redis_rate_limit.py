"""Integration tests for RedisRateLimitStore (real Redis/Valkey, real Lua)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest_asyncio

from forze.application.contracts.resilience import RateLimitStrategy
from forze_redis.adapters.rate_limit import RedisRateLimitStore
from forze_redis.kernel.client import RedisClient

# ----------------------- #

_KEY = ("p", "r")


def _strat(*, permits: int = 2, per_s: float = 1.0) -> RateLimitStrategy:
    return RateLimitStrategy(permits=permits, per=timedelta(seconds=per_s))


@pytest_asyncio.fixture(scope="function")
async def rate_limit_store(redis_client: RedisClient) -> RedisRateLimitStore:
    return RedisRateLimitStore(
        client=redis_client,
        namespace=f"it:ratelimit:{uuid4().hex[:12]}",
    )


class TestRedisRateLimitLua:
    async def test_bucket_starts_full_drains_and_rejects(
        self,
        rate_limit_store: RedisRateLimitStore,
    ) -> None:
        strat = _strat(permits=2)

        assert await rate_limit_store.try_acquire(_KEY, strat) is True
        assert await rate_limit_store.try_acquire(_KEY, strat) is True
        assert await rate_limit_store.try_acquire(_KEY, strat) is False

    async def test_refills_on_server_clock(
        self,
        rate_limit_store: RedisRateLimitStore,
    ) -> None:
        # 20 permits/s: drain the burst, then ~0.2s refills a few tokens.
        strat = _strat(permits=20, per_s=1.0)

        for _ in range(20):
            await rate_limit_store.try_acquire(_KEY, strat)

        assert await rate_limit_store.try_acquire(_KEY, strat) is False

        await asyncio.sleep(0.2)

        assert await rate_limit_store.try_acquire(_KEY, strat) is True

    async def test_two_stores_share_the_fleet_bucket(
        self,
        redis_client: RedisClient,
        rate_limit_store: RedisRateLimitStore,
    ) -> None:
        # A second "replica" pointing at the same namespace consumes the same
        # bucket: the declared rate is the fleet's rate, not per-replica.
        replica = RedisRateLimitStore(
            client=redis_client,
            namespace=rate_limit_store.namespace,
        )
        strat = _strat(permits=2)

        assert await rate_limit_store.try_acquire(_KEY, strat) is True
        assert await replica.try_acquire(_KEY, strat) is True
        assert await rate_limit_store.try_acquire(_KEY, strat) is False
        assert await replica.try_acquire(_KEY, strat) is False

    async def test_buckets_are_keyed_per_policy_and_route(
        self,
        rate_limit_store: RedisRateLimitStore,
    ) -> None:
        strat = _strat(permits=1)

        assert await rate_limit_store.try_acquire(("p", "a"), strat) is True
        assert await rate_limit_store.try_acquire(("p", "a"), strat) is False
        assert await rate_limit_store.try_acquire(("p", "b"), strat) is True
        assert await rate_limit_store.try_acquire(("q", "a"), strat) is True
