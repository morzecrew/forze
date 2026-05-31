"""Redis platform client lifecycle and health edge cases."""

from __future__ import annotations

import pytest

from forze.base.exceptions import CoreException
from forze_redis.kernel.client import RedisClient, RedisConfig


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_health_without_initialize() -> None:
    client = RedisClient()
    msg, ok = await client.health()
    assert ok is False
    assert msg


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_close_without_initialize_is_noop() -> None:
    client = RedisClient()
    await client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_initialize_is_idempotent(redis_container) -> None:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    dsn = f"redis://{host}:{port}/0"

    client = RedisClient()
    await client.initialize(dsn=dsn, config=RedisConfig(max_size=3))
    await client.initialize(dsn=dsn, config=RedisConfig(max_size=3))
    assert (await client.health())[1] is True
    await client.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_redis_get_after_close_raises(redis_container) -> None:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    dsn = f"redis://{host}:{port}/0"

    client = RedisClient()
    await client.initialize(dsn=dsn, config=RedisConfig(max_size=3))
    await client.close()

    with pytest.raises(CoreException, match="not initialized"):
        await client.get("missing-key")
