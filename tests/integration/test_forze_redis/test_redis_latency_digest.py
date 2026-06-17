"""Integration tests for RedisLatencyDigestStore (real Redis/Valkey, real Lua)."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest_asyncio

from forze.application.contracts.resilience import AdaptiveBulkheadStrategy
from forze_redis.adapters.latency_digest import RedisLatencyDigestStore
from forze_redis.kernel.client import RedisClient

# ----------------------- #

_KEY = ("p", "r")
_ALPHA = 0.01


def _strat(quantile: float = 0.5) -> AdaptiveBulkheadStrategy:
    return AdaptiveBulkheadStrategy(
        latency_threshold=timedelta(milliseconds=100),
        max_concurrency=8,
        latency_quantile=quantile,
    )


@pytest_asyncio.fixture(scope="function")
async def digest_store(redis_client: RedisClient) -> RedisLatencyDigestStore:
    # local_cache_ttl=0 -> every call re-reads real Redis (exercises the Lua).
    return RedisLatencyDigestStore(
        client=redis_client,
        namespace=f"it:latency:{uuid4().hex[:12]}",
        relative_accuracy=_ALPHA,
        local_cache_ttl=0.0,
    )


# ----------------------- #


class TestRedisLatencyDigestLua:
    async def test_warming_then_quantile(
        self,
        digest_store: RedisLatencyDigestStore,
    ) -> None:
        strat = _strat(quantile=0.5)

        # Below the warmup floor (5 samples): no signal.
        for _ in range(4):
            assert await digest_store.observe(_KEY, 0.1, strat) is None

        # The fifth sample crosses the floor; with a constant latency the
        # median estimate is within alpha of it.
        value = await digest_store.observe(_KEY, 0.1, strat)
        assert value is not None
        assert abs(value - 0.1) / 0.1 <= _ALPHA

    async def test_quantile_tracks_distribution(
        self,
        digest_store: RedisLatencyDigestStore,
    ) -> None:
        strat = _strat(quantile=0.95)

        # 90 samples at 0.05s and 10 at 1.0s: the top 10% (the p95 rank) sits in
        # the high cluster, so the merged digest's p95 estimate is ~1.0s.
        for _ in range(90):
            await digest_store.observe(_KEY, 0.05, strat)

        value = None
        for _ in range(10):
            value = await digest_store.observe(_KEY, 1.0, strat)

        assert value is not None
        assert abs(value - 1.0) / 1.0 <= _ALPHA

    async def test_cross_replica_merge(
        self,
        redis_client: RedisClient,
    ) -> None:
        """Two stores sharing a namespace merge into one fleet-wide digest."""

        namespace = f"it:latency:{uuid4().hex[:12]}"
        strat = _strat(quantile=0.5)

        replica_a = RedisLatencyDigestStore(
            client=redis_client,
            namespace=namespace,
            relative_accuracy=_ALPHA,
            local_cache_ttl=0.0,
        )
        replica_b = RedisLatencyDigestStore(
            client=redis_client,
            namespace=namespace,
            relative_accuracy=_ALPHA,
            local_cache_ttl=0.0,
        )

        # Each replica records into the same shared hash.
        for _ in range(50):
            await replica_a.observe(_KEY, 0.2, strat)

        for _ in range(50):
            await replica_b.observe(_KEY, 0.2, strat)

        # Either replica reads the merged median (~0.2) over all 100 samples.
        value = await replica_b.observe(_KEY, 0.2, strat)
        assert value is not None
        assert abs(value - 0.2) / 0.2 <= _ALPHA

    async def test_reset_opens_fresh_epoch(
        self,
        digest_store: RedisLatencyDigestStore,
    ) -> None:
        strat = _strat(quantile=0.5)

        for _ in range(10):
            await digest_store.observe(_KEY, 0.5, strat)

        await digest_store.reset(_KEY, strat)

        # After the reset the digest is empty again: back to warming.
        for _ in range(4):
            assert await digest_store.observe(_KEY, 0.5, strat) is None
