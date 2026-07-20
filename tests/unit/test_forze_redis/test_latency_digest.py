"""Unit tests for RedisLatencyDigestStore orchestration (mocked run_script)."""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock

from forze.application.contracts.resilience import AdaptiveBulkheadStrategy
from forze.application.execution.resilience import InMemoryLatencyDigestStore
from forze.base.primitives import DDSketch
from forze_redis.adapters.latency_digest import RedisLatencyDigestStore

# ----------------------- #

_KEY = ("p", "r")
_ALPHA = 0.01


def _strat() -> AdaptiveBulkheadStrategy:
    return AdaptiveBulkheadStrategy(
        latency_threshold=timedelta(milliseconds=100),
        max_concurrency=8,
        latency_quantile=0.95,
    )


def _store(run_script: AsyncMock, clock=None) -> RedisLatencyDigestStore:
    client = AsyncMock()
    client.run_script = run_script

    return RedisLatencyDigestStore(
        client=client,
        relative_accuracy=_ALPHA,
        clock=clock or (lambda: 100.0),
    )


# ----------------------- #


class TestObserve:
    async def test_warming_returns_none(self) -> None:
        # record -> "OK", quantile -> "" (fewer than min samples)
        rs = AsyncMock(side_effect=["OK", ""])
        store = _store(rs)

        assert await store.observe(_KEY, 0.05, _strat()) is None

    async def test_quantile_index_maps_back_to_latency(self) -> None:
        bucketer = DDSketch(relative_accuracy=_ALPHA)
        index = bucketer.index(0.2)
        rs = AsyncMock(side_effect=["OK", str(index)])
        store = _store(rs)

        value = await store.observe(_KEY, 0.2, _strat())

        # The store maps the Lua-returned bucket index back through the same
        # DDSketch bucketing — within alpha of the recorded latency.
        assert value is not None
        assert abs(value - 0.2) / 0.2 <= _ALPHA

    async def test_quantile_read_is_cached(self) -> None:
        index = DDSketch(relative_accuracy=_ALPHA).index(0.3)
        # 1st observe: record + quantile. 2nd observe within TTL: record only.
        rs = AsyncMock(side_effect=["OK", str(index), "OK"])
        store = _store(rs)

        first = await store.observe(_KEY, 0.3, _strat())
        second = await store.observe(_KEY, 0.3, _strat())

        assert first == second
        assert rs.await_count == 3  # not 4 — the quantile read was cached

    async def test_record_args_use_shared_bucketing(self) -> None:
        rs = AsyncMock(side_effect=["OK", ""])
        store = _store(rs)

        await store.observe(_KEY, 0.5, _strat())

        record_call = rs.await_args_list[0]
        sent_index = record_call.args[2][0]

        assert sent_index == str(DDSketch(relative_accuracy=_ALPHA).index(0.5))


class TestNonPositiveLatency:
    async def test_zero_latency_not_recorded_or_degraded(self) -> None:
        # A zero-duration sample must NOT be misclassified as a Redis failure
        # (the bucketing rejects <= 0) — no record, no degrade, no fallback.
        rs = AsyncMock()
        store = _store(rs)

        assert await store.observe(_KEY, 0.0, _strat()) is None
        rs.assert_not_awaited()

    async def test_negative_latency_not_recorded(self) -> None:
        rs = AsyncMock()
        store = _store(rs)

        assert await store.observe(_KEY, -0.5, _strat()) is None
        rs.assert_not_awaited()


class TestFailOpen:
    async def test_record_failure_falls_back_in_memory(self) -> None:
        rs = AsyncMock(side_effect=RuntimeError("redis down"))
        fallback = InMemoryLatencyDigestStore()
        client = AsyncMock()
        client.run_script = rs
        store = RedisLatencyDigestStore(
            client=client, relative_accuracy=_ALPHA, fallback=fallback
        )

        # Does not raise; routes to the in-memory fallback (warming -> None).
        assert await store.observe(_KEY, 0.05, _strat()) is None

    async def test_quantile_failure_falls_back(self) -> None:
        # record OK, quantile read raises.
        rs = AsyncMock(side_effect=["OK", RuntimeError("redis down")])
        store = _store(rs)

        assert await store.observe(_KEY, 0.05, _strat()) is None

    async def test_malformed_quantile_result_degrades_not_raises(self) -> None:
        # record OK, but the quantile read returns a non-integer: a successful
        # Redis call with a bad payload must fail-open, not propagate and break
        # the call whose work already succeeded.
        rs = AsyncMock(side_effect=["OK", "garbage"])
        store = _store(rs)

        assert await store.observe(_KEY, 0.05, _strat()) is None


class TestReset:
    async def test_reset_clears_cache_and_drops_hash(self) -> None:
        index = DDSketch(relative_accuracy=_ALPHA).index(0.3)
        rs = AsyncMock(side_effect=["OK", str(index), "OK", "OK", str(index)])
        store = _store(rs)

        await store.observe(_KEY, 0.3, _strat())  # populates the cache
        await store.reset(_KEY, _strat())  # DEL + cache clear
        await store.observe(_KEY, 0.3, _strat())  # must re-read the quantile

        # record, quantile, reset, record, quantile = 5 calls (cache was cleared)
        assert rs.await_count == 5

    async def test_reset_failure_is_swallowed(self) -> None:
        rs = AsyncMock(side_effect=RuntimeError("redis down"))
        store = _store(rs)

        # Fail-open: reset never raises.
        await store.reset(_KEY, _strat())
