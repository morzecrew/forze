import pytest
from unittest.mock import AsyncMock, MagicMock

from forze.base.errors import CoreError

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
async def test_mset_with_flags_uses_atomic_script(redis_client: RedisClient) -> None:
    mapping = {"key1": "val1", "key2": b"v2"}

    redis_client.run_script = AsyncMock(return_value="1")  # type: ignore[method-assign]

    res = await redis_client.mset(mapping, ex=10, nx=True)

    assert res is True
    redis_client.run_script.assert_awaited_once()
    script, keys, argv = redis_client.run_script.call_args[0]
    assert "set_one" in script
    assert keys == ["key1", "key2"]
    assert argv[0] == "10"
    assert argv[1] == "-1"
    assert argv[2] == "1"
    assert argv[3] == "0"
    assert argv[4] == "val1"
    assert argv[5] == b"v2"


@pytest.mark.asyncio
async def test_mset_nx_and_xx_rejected(redis_client: RedisClient) -> None:
    redis_client.run_script = AsyncMock(return_value="1")  # type: ignore[method-assign]

    with pytest.raises(CoreError, match="nx and xx"):
        await redis_client.mset({"a": "1"}, nx=True, xx=True)

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
