from uuid import uuid4

import pytest

from forze_redis.kernel.platform.client import RedisClient


@pytest.mark.asyncio
async def test_basic_kv_methods(redis_client: RedisClient) -> None:
    prefix = f"it:redis-client:{uuid4()}"
    key_1 = f"{prefix}:k1"
    key_2 = f"{prefix}:k2"
    key_3 = f"{prefix}:k3"

    assert await redis_client.set(key_1, "v1")
    assert await redis_client.get(key_1) == b"v1"

    assert await redis_client.mset({key_2: "v2", key_3: "v3"})
    values = await redis_client.mget([key_1, key_2, key_3, f"{prefix}:missing"])
    assert values == [b"v1", b"v2", b"v3", None]

    assert await redis_client.delete(key_1, key_2) == 2
    assert await redis_client.unlink(key_3) == 1

    assert await redis_client.get(key_1) is None
    assert await redis_client.get(key_2) is None
    assert await redis_client.get(key_3) is None


@pytest.mark.asyncio
async def test_counter_methods(redis_client: RedisClient) -> None:
    key = f"it:redis-client:counter:{uuid4()}"

    assert await redis_client.incr(key) == 1
    assert await redis_client.incr(key, by=4) == 5
    assert await redis_client.decr(key, by=2) == 3
    assert await redis_client.reset(key, value=10) == 3
    assert await redis_client.get(key) == b"10"


@pytest.mark.asyncio
async def test_nested_pipeline_reuses_parent(redis_client: RedisClient) -> None:
    prefix = f"it:redis-client:pipeline:{uuid4()}"
    key_1 = f"{prefix}:k1"
    key_2 = f"{prefix}:k2"

    async with redis_client.pipeline(transaction=True):
        await redis_client.set(key_1, "v1")

        async with redis_client.pipeline(transaction=True):
            await redis_client.set(key_2, "v2")

    assert await redis_client.get(key_1) == b"v1"
    assert await redis_client.get(key_2) == b"v2"


@pytest.mark.asyncio
async def test_health_reports_ok(redis_client: RedisClient) -> None:
    """health returns success when the server responds to ping."""
    status, ok = await redis_client.health()
    assert status == "ok"
    assert ok is True


@pytest.mark.asyncio
async def test_set_with_expiry(redis_client: RedisClient) -> None:
    """set with ex stores a key that expires."""
    prefix = f"it:redis-client:ttl:{uuid4()}"
    key = f"{prefix}:k"

    assert await redis_client.set(key, "v", ex=1)
    assert await redis_client.get(key) == b"v"


@pytest.mark.asyncio
async def test_mset_with_ex_sets_all_keys(redis_client: RedisClient) -> None:
    prefix = f"it:redis-client:mset-ex:{uuid4()}"
    a, b = f"{prefix}:a", f"{prefix}:b"

    assert await redis_client.mset({a: "1", b: "2"}, ex=3600) is True
    assert await redis_client.get(a) == b"1"
    assert await redis_client.get(b) == b"2"
    assert await redis_client.pttl(a) is not None


@pytest.mark.asyncio
async def test_mset_nx_all_or_nothing(redis_client: RedisClient) -> None:
    prefix = f"it:redis-client:mset-nx:{uuid4()}"
    a, b, c = f"{prefix}:a", f"{prefix}:b", f"{prefix}:c"

    await redis_client.set(c, "exists", ex=3600)

    ok = await redis_client.mset({a: "na", b: "nb", c: "nc"}, ex=60, nx=True)
    assert ok is False
    assert await redis_client.get(a) is None
    assert await redis_client.get(b) is None
    assert await redis_client.get(c) == b"exists"

    assert await redis_client.mset({a: "x", b: "y"}, ex=60, nx=True) is True
    assert await redis_client.get(a) == b"x"
    assert await redis_client.get(b) == b"y"
