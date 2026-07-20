"""Unit tests for RedisRateLimitStore orchestration (mocked run_script)."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

from forze.application.contracts.resilience import RateLimitStrategy
from forze.application.execution.resilience import InMemoryRateLimitStore
from forze.base.exceptions import exc
from forze_redis.adapters.rate_limit import RedisRateLimitStore

# ----------------------- #

_KEY = ("p", "r")


def _strat(*, permits: int = 2, burst: int | None = None) -> RateLimitStrategy:
    return RateLimitStrategy(permits=permits, per=timedelta(seconds=1), burst=burst)


def _store(run_script: AsyncMock) -> RedisRateLimitStore:
    client = AsyncMock()
    client.run_script = run_script
    return RedisRateLimitStore(client=client)


class TestPayloadAndArgs:
    async def test_acquire_allowed(self) -> None:
        store = _store(AsyncMock(return_value="1"))
        assert await store.try_acquire(_KEY, _strat()) is True

    async def test_acquire_rejected(self) -> None:
        store = _store(AsyncMock(return_value="0"))
        assert await store.try_acquire(_KEY, _strat()) is False

    async def test_script_receives_rate_capacity_ttl(self) -> None:
        rs = AsyncMock(return_value="1")
        store = _store(rs)

        await store.try_acquire(_KEY, _strat(permits=2, burst=5))

        _script, keys, args = rs.await_args.args
        assert keys == ["forze:resilience:ratelimit:p:r"]
        rate, capacity, ttl_ms = args
        assert rate == 2.0
        assert capacity == 5
        assert ttl_ms >= 60_000

    async def test_routeless_key_uses_placeholder(self) -> None:
        rs = AsyncMock(return_value="1")
        store = _store(rs)

        await store.try_acquire(("p", None), _strat())

        _script, keys, _args = rs.await_args.args
        assert keys == ["forze:resilience:ratelimit:p:-"]


class TestFailOpen:
    async def test_falls_back_to_local_bucket_on_redis_error(self) -> None:
        store = _store(AsyncMock(side_effect=exc.infrastructure("redis down")))
        strat = _strat(permits=1)

        # Degraded mode still rate-limits per replica via the local fallback.
        assert await store.try_acquire(_KEY, strat) is True
        assert await store.try_acquire(_KEY, strat) is False

    async def test_fallback_is_replaceable(self) -> None:
        fallback = InMemoryRateLimitStore()
        client = AsyncMock()
        client.run_script = AsyncMock(side_effect=RuntimeError("boom"))
        store = RedisRateLimitStore(client=client, fallback=fallback)

        assert await store.try_acquire(_KEY, _strat(permits=1)) is True
        assert ("p", "r") in fallback._states
