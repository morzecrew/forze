"""Unit tests for RedisCircuitBreakerStore orchestration (mocked run_script)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta
from unittest.mock import AsyncMock

from forze.application.contracts.resilience import CircuitBreakerStrategy
from forze.application.execution.resilience import InMemoryCircuitBreakerStore
from forze.base.exceptions import exc
from forze_redis.adapters.circuit_breaker import RedisCircuitBreakerStore

# ----------------------- #


def _strat() -> CircuitBreakerStrategy:
    return CircuitBreakerStrategy(
        failure_ratio=0.5,
        sampling_window=timedelta(seconds=10),
        min_throughput=2,
        break_duration=timedelta(seconds=5),
    )


def _store(
    run_script: AsyncMock,
    clock: Callable[[], float] | None = None,
) -> RedisCircuitBreakerStore:
    client = AsyncMock()
    client.run_script = run_script
    return RedisCircuitBreakerStore(client=client, clock=clock or (lambda: 100.0))


_KEY = ("p", "r")


class TestPayloadParsing:
    async def test_admit_allowed(self) -> None:
        store = _store(AsyncMock(return_value="1:closed:none"))
        assert await store.admit(_KEY, _strat()) == (True, None)

    async def test_admit_open_denied(self) -> None:
        store = _store(AsyncMock(return_value="0:open:none"))
        assert await store.admit(_KEY, _strat()) == (False, None)

    async def test_admit_half_open_transition(self) -> None:
        store = _store(AsyncMock(return_value="1:half_open:half_open"))
        assert await store.admit(_KEY, _strat()) == (True, "half_open")

    async def test_record_transition(self) -> None:
        rs = AsyncMock(return_value="open:open")
        store = _store(rs)
        assert await store.record(_KEY, _strat(), False) == "open"
        rs.assert_awaited_once()


class TestLocalCacheFastPath:
    async def test_closed_admit_skips_redis_within_ttl(self) -> None:
        rs = AsyncMock(return_value="1:closed:none")
        now = [100.0]
        store = _store(rs, clock=lambda: now[0])
        strat = _strat()

        await store.admit(_KEY, strat)  # populates cache (closed @ +0.25)
        assert rs.await_count == 1

        await store.admit(_KEY, strat)  # within TTL -> no Redis
        assert rs.await_count == 1

        now[0] += 1.0  # cache stale -> Redis again
        await store.admit(_KEY, strat)
        assert rs.await_count == 2

    async def test_open_phase_is_not_fast_pathed(self) -> None:
        rs = AsyncMock(return_value="0:open:none")
        store = _store(rs)
        strat = _strat()

        await store.admit(_KEY, strat)
        await store.admit(_KEY, strat)  # open is never cached as fast-path
        assert rs.await_count == 2


class TestLocalCacheBound:
    async def test_cache_size_is_capped_with_fifo_eviction(self) -> None:
        rs = AsyncMock(return_value="1:closed:none")
        client = AsyncMock()
        client.run_script = rs
        store = RedisCircuitBreakerStore(
            client=client, clock=lambda: 100.0, max_cache_entries=3
        )
        strat = _strat()

        # Five distinct routes (e.g. per-host) admitted; the fast-path cache holds
        # at most the cap, evicting the oldest inserted entries.
        for i in range(5):
            await store.admit(("p", f"route-{i}"), strat)

        cache = store._cache
        assert len(cache) == 3
        # FIFO: the two oldest (route-0, route-1) were evicted.
        assert ("p", "route-0") not in cache
        assert ("p", "route-4") in cache

    async def test_existing_key_update_does_not_grow_cache(self) -> None:
        rs = AsyncMock(return_value="1:closed:none")
        client = AsyncMock()
        client.run_script = rs
        now = [100.0]
        store = RedisCircuitBreakerStore(
            client=client, clock=lambda: now[0], max_cache_entries=2
        )
        strat = _strat()

        await store.admit(("p", "a"), strat)
        now[0] += 1.0  # expire the fast-path so admit re-runs and re-remembers
        await store.admit(("p", "a"), strat)

        cache = store._cache
        assert len(cache) == 1


class TestFailOpen:
    async def test_admit_falls_back_to_local_on_redis_error(self) -> None:
        rs = AsyncMock(side_effect=exc.infrastructure("redis down"))
        fallback = InMemoryCircuitBreakerStore(clock=lambda: 100.0)
        client = AsyncMock()
        client.run_script = rs
        store = RedisCircuitBreakerStore(
            client=client, fallback=fallback, clock=lambda: 100.0
        )

        # Never raises; degrades to the process-local fallback (closed -> allowed).
        assert await store.admit(_KEY, _strat()) == (True, None)

    async def test_record_falls_back_to_local_on_redis_error(self) -> None:
        rs = AsyncMock(side_effect=exc.infrastructure("redis down"))
        fallback = InMemoryCircuitBreakerStore(clock=lambda: 100.0)
        client = AsyncMock()
        client.run_script = rs
        store = RedisCircuitBreakerStore(
            client=client, fallback=fallback, clock=lambda: 100.0
        )

        # First failure is below min_throughput in the fallback -> no trip, no raise.
        assert await store.record(_KEY, _strat(), False) is None
