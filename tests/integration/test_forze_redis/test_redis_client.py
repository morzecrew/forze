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
