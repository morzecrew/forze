"""A full pool refuses the next caller at once, or makes it wait up to ``pool_timeout``."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import timedelta

import pytest

from forze.base.exceptions import CoreException
from forze_redis.kernel.client import RedisClient, RedisConfig

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_HOLD_MS = 400


@pytest.fixture
async def open_client(
    redis_container,
) -> AsyncIterator[Callable[[RedisConfig], Awaitable[RedisClient]]]:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    clients: list[RedisClient] = []

    async def _open(config: RedisConfig) -> RedisClient:
        client = RedisClient()
        await client.initialize(dsn=f"redis://{host}:{port}/0", config=config)
        clients.append(client)
        return client

    yield _open

    for client in clients:
        await client.close()


async def _hold_the_only_connection(client: RedisClient) -> asyncio.Task[object]:
    """Block the pool's single connection in a stream read nobody writes to."""

    async def _read() -> object:
        return await client.xread({"pool-timeout:idle": "$"}, block_ms=_HOLD_MS)

    holder = asyncio.create_task(_read())
    await asyncio.sleep(0.05)
    return holder


# ....................... #


async def test_a_full_pool_refuses_at_once_by_default(open_client) -> None:
    client = await open_client(RedisConfig(max_size=1))
    holder = await _hold_the_only_connection(client)

    started = time.monotonic()

    with pytest.raises(CoreException, match="Redis connection error"):
        await client.get("pool-timeout:any")

    assert time.monotonic() - started < _HOLD_MS / 1000 / 2
    await holder


async def test_a_full_pool_waits_for_a_released_connection(open_client) -> None:
    client = await open_client(RedisConfig(max_size=1, pool_timeout=timedelta(seconds=5)))
    holder = await _hold_the_only_connection(client)

    assert await client.get("pool-timeout:any") is None
    assert holder.done()


async def test_a_full_pool_waits_no_longer_than_its_timeout(open_client) -> None:
    client = await open_client(RedisConfig(max_size=1, pool_timeout=timedelta(milliseconds=50)))
    holder = await _hold_the_only_connection(client)

    with pytest.raises(CoreException, match="Redis connection error"):
        await client.get("pool-timeout:any")

    assert not holder.done()
    await holder
