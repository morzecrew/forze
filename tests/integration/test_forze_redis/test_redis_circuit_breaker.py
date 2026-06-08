"""Integration tests for RedisCircuitBreakerStore (real Redis/Valkey, real Lua)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest_asyncio

from forze.application.contracts.resilience import CircuitBreakerStrategy
from forze_redis.adapters.circuit_breaker import RedisCircuitBreakerStore
from forze_redis.kernel.client import RedisClient

# ----------------------- #

_KEY = ("p", "r")


def _strat() -> CircuitBreakerStrategy:
    return CircuitBreakerStrategy(
        failure_ratio=0.5,
        sampling_window=timedelta(seconds=10),
        min_throughput=2,
        break_duration=timedelta(seconds=1),
        half_open_max_calls=1,
    )


@pytest_asyncio.fixture(scope="function")
async def breaker_store(redis_client: RedisClient) -> RedisCircuitBreakerStore:
    # local_cache_ttl=0 -> every call hits real Redis (exercises the Lua directly).
    return RedisCircuitBreakerStore(
        client=redis_client,
        namespace=f"it:breaker:{uuid4().hex[:12]}",
        local_cache_ttl=0.0,
    )


class TestRedisCircuitBreakerLua:
    async def test_open_half_open_close_lifecycle(
        self,
        breaker_store: RedisCircuitBreakerStore,
    ) -> None:
        strat = _strat()

        allowed, tr = await breaker_store.admit(_KEY, strat)
        assert allowed is True
        assert tr is None

        assert await breaker_store.record(_KEY, strat, False) is None
        assert await breaker_store.record(_KEY, strat, False) == "open"

        allowed, _ = await breaker_store.admit(_KEY, strat)
        assert allowed is False  # open, within break window

        await asyncio.sleep(1.2)  # past break_duration -> half-open
        allowed, tr = await breaker_store.admit(_KEY, strat)
        assert allowed is True
        assert tr == "half_open"

        assert await breaker_store.record(_KEY, strat, True) == "closed"

        allowed, tr = await breaker_store.admit(_KEY, strat)
        assert allowed is True
        assert tr is None

    async def test_half_open_failure_reopens(
        self,
        breaker_store: RedisCircuitBreakerStore,
    ) -> None:
        strat = _strat()

        await breaker_store.record(_KEY, strat, False)
        await breaker_store.record(_KEY, strat, False)  # open
        await asyncio.sleep(1.2)
        _, tr = await breaker_store.admit(_KEY, strat)
        assert tr == "half_open"

        assert await breaker_store.record(_KEY, strat, False) == "open"

    async def test_two_stores_share_the_trip(
        self,
        redis_client: RedisClient,
    ) -> None:
        ns = f"it:breaker:{uuid4().hex[:12]}"
        strat = _strat()

        a = RedisCircuitBreakerStore(client=redis_client, namespace=ns, local_cache_ttl=0.0)
        b = RedisCircuitBreakerStore(client=redis_client, namespace=ns, local_cache_ttl=0.0)

        await a.record(_KEY, strat, False)
        assert await a.record(_KEY, strat, False) == "open"  # replica A trips it

        allowed, _ = await b.admit(_KEY, strat)
        assert allowed is False  # replica B sees the shared open state
