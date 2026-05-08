"""Integration tests for Redis distributed lock adapter and coordinator."""

from datetime import timedelta

import pytest

from forze.application.coordinators import DistributedLockCoordinator
from forze_redis.adapters import RedisDistributedLockAdapter


@pytest.mark.asyncio
async def test_adapter_acquire_query_release(redis_dlock: RedisDistributedLockAdapter) -> None:
    assert await redis_dlock.acquire("job", "worker-a") is True
    assert await redis_dlock.is_locked("job") is True
    assert await redis_dlock.get_owner("job") == "worker-a"

    ttl = await redis_dlock.get_ttl("job")
    assert ttl is not None
    assert ttl.total_seconds() > 0

    assert await redis_dlock.release("job", "worker-a") is True
    assert await redis_dlock.is_locked("job") is False


@pytest.mark.asyncio
async def test_adapter_release_wrong_owner_noops(redis_dlock: RedisDistributedLockAdapter) -> None:
    assert await redis_dlock.acquire("x", "alice") is True
    assert await redis_dlock.release("x", "bob") is False
    assert await redis_dlock.is_locked("x") is True
    assert await redis_dlock.release("x", "alice") is True


@pytest.mark.asyncio
async def test_coordinator_scope_releases_lock(
    redis_dlock: RedisDistributedLockAdapter,
) -> None:
    coord = DistributedLockCoordinator(
        cmd=redis_dlock,
        owner_provider=lambda: "coord-owner",
        wait_timeout=timedelta(seconds=5),
    )

    async with coord.scope("exclusive-task"):
        assert await redis_dlock.is_locked("exclusive-task") is True

    assert await redis_dlock.is_locked("exclusive-task") is False
