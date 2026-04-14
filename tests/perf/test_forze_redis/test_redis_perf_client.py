"""Performance tests for RedisClient."""

from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze_redis.kernel.platform.client import RedisClient

_REDIS_MGET_LARGE = 1_000
_REDIS_MSET_LARGE = 500
_REDIS_PIPELINE_LARGE = 100


def _perf_key(prefix: str, suffix: str = "") -> str:
    return f"perf:{prefix}:{uuid4().hex[:8]}{suffix}"


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_get_benchmark(async_benchmark, redis_client: RedisClient) -> None:
    """Benchmark single get."""
    key = _perf_key("get")
    await redis_client.set(key, b"value")

    async def run() -> None:
        val = await redis_client.get(key)
        assert val == b"value"

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_set_benchmark(async_benchmark, redis_client: RedisClient) -> None:
    """Benchmark single set."""

    async def run() -> None:
        key = _perf_key("set")
        await redis_client.set(key, b"value")
        await redis_client.delete(key)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_mget_small_benchmark(
    async_benchmark, redis_client: RedisClient
) -> None:
    """Benchmark mget with 10 keys."""
    keys = [_perf_key("mget", f":{i}") for i in range(10)]
    for k in keys:
        await redis_client.set(k, b"v")

    async def run() -> None:
        vals = await redis_client.mget(keys)
        assert len(vals) == 10

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_mget_medium_benchmark(
    async_benchmark, redis_client: RedisClient
) -> None:
    """Benchmark mget with 100 keys."""
    keys = [_perf_key("mget100", f":{i}") for i in range(100)]
    for k in keys:
        await redis_client.set(k, b"v")

    async def run() -> None:
        vals = await redis_client.mget(keys)
        assert len(vals) == 100

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_mget_large_benchmark(
    async_benchmark, redis_client: RedisClient
) -> None:
    """Benchmark mget with 1000 keys (high volume)."""
    keys = [_perf_key("mget1k", f":{i}") for i in range(_REDIS_MGET_LARGE)]
    for k in keys:
        await redis_client.set(k, b"v")

    async def run() -> None:
        vals = await redis_client.mget(keys)
        assert len(vals) == _REDIS_MGET_LARGE

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_mset_benchmark(async_benchmark, redis_client: RedisClient) -> None:
    """Benchmark mset with 20 key-value pairs."""

    async def run() -> None:
        mapping = {_perf_key("mset", f":{i}"): b"v" for i in range(20)}
        await redis_client.mset(mapping)
        await redis_client.delete(*mapping.keys())

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_mset_large_benchmark(
    async_benchmark, redis_client: RedisClient
) -> None:
    """Benchmark mset with 500 key-value pairs (high volume)."""

    async def run() -> None:
        mapping = {_perf_key("mset500", f":{i}"): b"v" for i in range(_REDIS_MSET_LARGE)}
        await redis_client.mset(mapping)
        await redis_client.delete(*mapping.keys())

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_pipeline_benchmark(
    async_benchmark, redis_client: RedisClient
) -> None:
    """Benchmark pipeline with 10 set operations."""

    async def run() -> None:
        keys = [_perf_key("pipe", f":{i}") for i in range(10)]
        async with redis_client.pipeline(transaction=True):
            for key in keys:
                await redis_client.set(key, b"v")
        await redis_client.delete(*keys)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_pipeline_large_benchmark(
    async_benchmark, redis_client: RedisClient
) -> None:
    """Benchmark pipeline with 100 set operations (high volume)."""

    async def run() -> None:
        keys = [_perf_key("pipe100", f":{i}") for i in range(_REDIS_PIPELINE_LARGE)]
        async with redis_client.pipeline(transaction=True):
            for key in keys:
                await redis_client.set(key, b"v")
        await redis_client.delete(*keys)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_incr_benchmark(async_benchmark, redis_client: RedisClient) -> None:
    """Benchmark incr."""
    key = _perf_key("incr")
    await redis_client.set(key, b"0")

    async def run() -> None:
        await redis_client.incr(key)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_delete_benchmark(async_benchmark, redis_client: RedisClient) -> None:
    """Benchmark delete of 5 keys."""

    async def run() -> None:
        keys = [_perf_key("del", f":{i}") for i in range(5)]
        for k in keys:
            await redis_client.set(k, b"v")
        await redis_client.delete(*keys)

    await async_benchmark(run)


@pytest.mark.perf
@pytest.mark.asyncio
async def test_redis_set_with_ttl_benchmark(
    async_benchmark, redis_client: RedisClient
) -> None:
    """Benchmark set with expiration."""

    async def run() -> None:
        key = _perf_key("ttl")
        await redis_client.set(key, b"v", ex=60)
        await redis_client.delete(key)

    await async_benchmark(run)
