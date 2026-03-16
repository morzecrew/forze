"""Performance tests for Redis adapters (cache, counter)."""

from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("redis")

from forze.base.codecs import KeyCodec
from forze_redis.adapters import RedisCacheAdapter, RedisCounterAdapter
from forze_redis.kernel.platform.client import RedisClient


def _perf_namespace(prefix: str) -> str:
    return f"perf:{prefix}:{uuid4().hex[:12]}"


@pytest_asyncio.fixture
async def redis_cache(redis_client: RedisClient) -> RedisCacheAdapter:
    """Provide a RedisCacheAdapter with a unique namespace per test."""
    return RedisCacheAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=_perf_namespace("cache")),
    )


@pytest_asyncio.fixture
async def redis_counter(redis_client: RedisClient) -> RedisCounterAdapter:
    """Provide a RedisCounterAdapter with a unique namespace per test."""
    return RedisCounterAdapter(
        client=redis_client,
        key_codec=KeyCodec(namespace=_perf_namespace("counter")),
        tenant_context=None,
    )


@pytest.mark.perf
@pytest.mark.asyncio
async def test_cache_set_benchmark(
    async_benchmark, redis_cache: RedisCacheAdapter
) -> None:
    """Benchmark cache set."""

    async def run() -> None:
        key = f"k:{uuid4().hex[:8]}"
        await redis_cache.set(key, {"data": "value"})
        await redis_cache.delete(key, hard=True)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_cache_get_benchmark(
    async_benchmark, redis_cache: RedisCacheAdapter
) -> None:
    """Benchmark cache get."""
    key = f"k:{uuid4().hex[:8]}"
    await redis_cache.set(key, {"data": "value"})

    async def run() -> None:
        val = await redis_cache.get(key)
        assert val == {"data": "value"}

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_cache_get_many_benchmark(
    async_benchmark, redis_cache: RedisCacheAdapter
) -> None:
    """Benchmark cache get_many with 10 keys."""
    keys = [f"k:{uuid4().hex[:8]}" for _ in range(10)]
    for k in keys:
        await redis_cache.set(k, {"data": "v"})

    async def run() -> None:
        hits, misses = await redis_cache.get_many(keys)
        assert len(hits) == 10
        assert len(misses) == 0

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_cache_set_many_benchmark(
    async_benchmark, redis_cache: RedisCacheAdapter
) -> None:
    """Benchmark cache set_many with 20 items."""

    async def run() -> None:
        mapping = {f"k:{uuid4().hex[:8]}": {"data": f"v{i}"} for i in range(20)}
        await redis_cache.set_many(mapping)
        await redis_cache.delete_many(list(mapping.keys()), hard=True)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_counter_incr_benchmark(
    async_benchmark, redis_counter: RedisCounterAdapter
) -> None:
    """Benchmark counter incr."""
    suffix = uuid4().hex[:8]
    await redis_counter.reset(0, suffix=suffix)

    async def run() -> None:
        await redis_counter.incr(suffix=suffix)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_counter_incr_batch_benchmark(
    async_benchmark, redis_counter: RedisCounterAdapter
) -> None:
    """Benchmark counter incr_batch with size 10."""
    suffix = uuid4().hex[:8]
    await redis_counter.reset(0, suffix=suffix)

    async def run() -> None:
        ids = await redis_counter.incr_batch(size=10, suffix=suffix)
        assert len(ids) == 10

    await async_benchmark(run)
