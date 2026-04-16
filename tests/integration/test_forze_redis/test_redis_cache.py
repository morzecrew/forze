"""Integration tests for RedisCacheAdapter."""

import pytest

from forze_redis.adapters import RedisCacheAdapter


@pytest.mark.asyncio
async def test_cache_set_and_get(redis_cache: RedisCacheAdapter) -> None:
    """Plain KV set and get round-trip."""
    await redis_cache.set("k1", {"foo": 42})
    result = await redis_cache.get("k1")
    assert result == {"foo": 42}


@pytest.mark.asyncio
async def test_cache_get_missing_returns_none(redis_cache: RedisCacheAdapter) -> None:
    """Missing key returns None."""
    result = await redis_cache.get("nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_cache_get_many(redis_cache: RedisCacheAdapter) -> None:
    """get_many returns hits and misses."""
    await redis_cache.set("a", 1)
    await redis_cache.set("b", 2)

    hits, misses = await redis_cache.get_many(["a", "b", "c"])
    assert hits == {"a": 1, "b": 2}
    assert misses == ["c"]


@pytest.mark.asyncio
async def test_cache_set_versioned_and_get(redis_cache: RedisCacheAdapter) -> None:
    """Versioned set and get round-trip."""
    await redis_cache.set_versioned("vkey", "v1", {"data": "first"})
    result = await redis_cache.get("vkey")
    assert result == {"data": "first"}


@pytest.mark.asyncio
async def test_cache_set_many(redis_cache: RedisCacheAdapter) -> None:
    """set_many stores multiple plain KV entries."""
    await redis_cache.set_many({"x": 10, "y": 20, "z": 30})
    hits, misses = await redis_cache.get_many(["x", "y", "z"])
    assert hits == {"x": 10, "y": 20, "z": 30}
    assert misses == []


@pytest.mark.asyncio
async def test_cache_set_many_versioned(redis_cache: RedisCacheAdapter) -> None:
    """set_many_versioned stores versioned entries."""
    await redis_cache.set_many_versioned(
        {("k1", "v1"): {"n": 1}, ("k2", "v1"): {"n": 2}}
    )
    result1 = await redis_cache.get("k1")
    result2 = await redis_cache.get("k2")
    assert result1 == {"n": 1}
    assert result2 == {"n": 2}


@pytest.mark.asyncio
async def test_cache_delete_soft(redis_cache: RedisCacheAdapter) -> None:
    """delete with hard=False removes current pointer and plain KV."""
    await redis_cache.set("d1", "plain")
    await redis_cache.delete("d1", hard=False)
    assert await redis_cache.get("d1") is None


@pytest.mark.asyncio
async def test_cache_delete_hard(redis_cache: RedisCacheAdapter) -> None:
    """delete with hard=True removes pointer, body, and plain KV."""
    await redis_cache.set_versioned("d2", "v1", {"x": 1})
    await redis_cache.delete("d2", hard=True)
    assert await redis_cache.get("d2") is None


@pytest.mark.asyncio
async def test_cache_delete_many(redis_cache: RedisCacheAdapter) -> None:
    """delete_many removes multiple keys."""
    await redis_cache.set_many({"m1": 1, "m2": 2, "m3": 3})
    await redis_cache.delete_many(["m1", "m2"], hard=False)
    hits, misses = await redis_cache.get_many(["m1", "m2", "m3"])
    assert hits == {"m3": 3}
    assert set(misses) == {"m1", "m2"}


@pytest.mark.asyncio
async def test_cache_get_many_mixes_versioned_and_plain(redis_cache: RedisCacheAdapter) -> None:
    """get_many can return both versioned entries and plain KV in one batch."""
    await redis_cache.set_versioned("ver", "1", {"kind": "v"})
    await redis_cache.set("plain", 42)

    hits, misses = await redis_cache.get_many(["ver", "plain", "absent"])

    assert hits["ver"] == {"kind": "v"}
    assert hits["plain"] == 42
    assert misses == ["absent"]


@pytest.mark.asyncio
async def test_cache_delete_many_hard_clears_versioned_bodies(redis_cache: RedisCacheAdapter) -> None:
    """delete_many with hard=True removes versioned data for all keys."""
    await redis_cache.set_many_versioned(
        {("a", "v1"): {"n": 1}, ("b", "v1"): {"n": 2}},
    )
    await redis_cache.delete_many(["a", "b"], hard=True)

    hits, misses = await redis_cache.get_many(["a", "b"])
    assert hits == {}
    assert set(misses) == {"a", "b"}
