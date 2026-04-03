"""Integration tests for RedisCounterAdapter."""

import pytest

from forze_redis.adapters import RedisCounterAdapter


@pytest.mark.asyncio
async def test_counter_incr(redis_counter: RedisCounterAdapter) -> None:
    """incr increments and returns new value."""
    v = await redis_counter.incr()
    assert v == 1
    v = await redis_counter.incr(by=4)
    assert v == 5


@pytest.mark.asyncio
async def test_counter_decr(redis_counter: RedisCounterAdapter) -> None:
    """decr decrements and returns new value."""
    await redis_counter.incr(by=10)
    v = await redis_counter.decr(by=3)
    assert v == 7


@pytest.mark.asyncio
async def test_counter_reset(redis_counter: RedisCounterAdapter) -> None:
    """reset sets value and returns previous."""
    await redis_counter.incr(by=5)
    prev = await redis_counter.reset(value=100)
    assert prev == 5
    v = await redis_counter.incr()
    assert v == 101


@pytest.mark.asyncio
async def test_counter_incr_batch(redis_counter: RedisCounterAdapter) -> None:
    """incr_batch allocates sequential values."""
    batch = await redis_counter.incr_batch(size=5)
    assert batch == [1, 2, 3, 4, 5]
    next_batch = await redis_counter.incr_batch(size=3)
    assert next_batch == [6, 7, 8]


@pytest.mark.asyncio
async def test_counter_incr_batch_size_one(
    redis_counter: RedisCounterAdapter,
) -> None:
    """incr_batch with size=1 returns a single allocated value."""
    one = await redis_counter.incr_batch(size=1)
    assert one == [1]
    two = await redis_counter.incr_batch(size=1)
    assert two == [2]


@pytest.mark.asyncio
async def test_counter_suffix_partitions(redis_counter: RedisCounterAdapter) -> None:
    """Different suffixes yield independent counters."""
    v_a = await redis_counter.incr(suffix="a")
    v_b = await redis_counter.incr(suffix="b")
    v_a2 = await redis_counter.incr(suffix="a")
    assert v_a == 1
    assert v_b == 1
    assert v_a2 == 2
