import pytest
from unittest.mock import AsyncMock, MagicMock
from forze_redis.kernel.platform.client import RedisClient

@pytest.fixture
def redis_client():
    client = RedisClient()
    # Mocking the internal state
    client._RedisClient__client = MagicMock()
    return client

@pytest.mark.asyncio
async def test_mset_native_call(redis_client):
    mapping = {"key1": "val1", "key2": "val2"}

    # We expect mset to be called on the executor (the client itself if no pipeline)
    client = redis_client._RedisClient__require_client()
    client.mset = AsyncMock(return_value=True)

    res = await redis_client.mset(mapping)

    assert res is True
    client.mset.assert_awaited_once_with(mapping)

@pytest.mark.asyncio
async def test_mset_with_flags_uses_pipeline(redis_client):
    mapping = {"key1": "val1"}

    # Mock pipeline
    pipe = AsyncMock()
    redis_client._RedisClient__require_client().pipeline.return_value = pipe

    res = await redis_client.mset(mapping, ex=10)

    assert res is True
    # Should NOT call native mset
    redis_client._RedisClient__require_client().mset.assert_not_called()
    # Should call pipe.set
    pipe.set.assert_awaited_once_with("key1", "val1", ex=10, px=None, nx=False, xx=False)
    # Should execute the pipeline
    pipe.execute.assert_awaited_once()

@pytest.mark.asyncio
async def test_mset_empty_mapping(redis_client):
    res = await redis_client.mset({})
    assert res is True
    redis_client._RedisClient__require_client().mset.assert_not_called()
    redis_client._RedisClient__require_client().pipeline.assert_not_called()

@pytest.mark.asyncio
async def test_mset_inside_pipeline_uses_native_mset(redis_client):
    mapping = {"key1": "val1", "key2": "val2"}

    pipe = AsyncMock()
    pipe.mset = AsyncMock(return_value=True)

    # Simulate being inside a pipeline context
    redis_client._RedisClient__ctx_pipe.set(pipe)
    redis_client._RedisClient__ctx_depth.set(1)

    res = await redis_client.mset(mapping)

    assert res is True
    # Native mset should be called ON THE PIPELINE (executor)
    pipe.mset.assert_awaited_once_with(mapping)
    # pipe.set should NOT be called (since no flags)
    pipe.set.assert_not_called()
