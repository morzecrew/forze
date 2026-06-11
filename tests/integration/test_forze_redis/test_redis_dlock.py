"""Integration tests for Redis distributed lock adapter and coordinator."""

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.dlock import AcquiredLock, DistributedLockSpec
from forze_kits.scopes import DistributedLockScope
from forze_redis.adapters import RedisDistributedLockAdapter


@pytest.mark.asyncio
async def test_adapter_acquire_query_release(
    redis_dlock: RedisDistributedLockAdapter,
) -> None:
    acquired = await redis_dlock.acquire("job", "worker-a")
    assert acquired == AcquiredLock(key="job", owner="worker-a", token=1)

    assert await redis_dlock.is_locked("job") is True
    assert await redis_dlock.get_owner("job") == "worker-a"

    ttl = await redis_dlock.get_ttl("job")
    assert ttl is not None
    assert ttl.total_seconds() > 0

    # Contention: the loser gets None.
    assert await redis_dlock.acquire("job", "worker-b") is None

    assert await redis_dlock.release("job", "worker-a") is True
    assert await redis_dlock.is_locked("job") is False


@pytest.mark.asyncio
async def test_adapter_release_wrong_owner_noops(
    redis_dlock: RedisDistributedLockAdapter,
) -> None:
    assert await redis_dlock.acquire("x", "alice") is not None
    assert await redis_dlock.release("x", "bob") is False
    assert await redis_dlock.is_locked("x") is True
    assert await redis_dlock.release("x", "alice") is True


@pytest.mark.asyncio
async def test_fencing_tokens_increase_across_generations(
    redis_dlock: RedisDistributedLockAdapter,
) -> None:
    """Two sequential lock generations on the same key get strictly increasing tokens."""
    first = await redis_dlock.acquire("gen", "owner-1")
    assert first is not None and first.token == 1
    assert await redis_dlock.release("gen", "owner-1") is True

    second = await redis_dlock.acquire("gen", "owner-2")
    assert second is not None and second.token == 2
    assert second.token > first.token

    # The fencing counter survives release (no TTL, never deleted).
    fence_key = redis_dlock.construct_key("dlock", "gen", "fence")
    assert await redis_dlock.client.exists(fence_key) is True

    await redis_dlock.release("gen", "owner-2")
    assert await redis_dlock.client.exists(fence_key) is True


@pytest.mark.asyncio
async def test_fencing_token_higher_after_ttl_expiry(
    redis_dlock: RedisDistributedLockAdapter,
) -> None:
    """A holder acquiring after the previous lease expired gets a HIGHER token.

    The fencing regression: the stale (GC/network-paused) holder's token is now
    provably stale to any store that tracks the highest observed token.
    """
    short = RedisDistributedLockAdapter(
        client=redis_dlock.client,
        namespace=redis_dlock.namespace,
        spec=DistributedLockSpec(name="it-lock-short", ttl=timedelta(milliseconds=80)),
    )

    stale = await short.acquire("paused", "stale-holder")
    assert stale is not None and stale.token is not None

    # Simulate a paused holder: the lease expires without a release.
    await asyncio.sleep(0.15)
    assert await short.is_locked("paused") is False

    fresh = await short.acquire("paused", "new-holder")
    assert fresh is not None and fresh.token is not None
    assert fresh.token > stale.token


@pytest.mark.asyncio
async def test_coordinator_scope_releases_lock(
    redis_dlock: RedisDistributedLockAdapter,
) -> None:
    coord = DistributedLockScope(
        cmd=redis_dlock,
        owner_provider=lambda: "coord-owner",
        wait_timeout=timedelta(seconds=5),
    )

    async with coord.scope("exclusive-task") as held:
        assert held.key == "exclusive-task"
        assert held.owner == "coord-owner"
        assert held.token == 1
        assert await redis_dlock.is_locked("exclusive-task") is True

    assert await redis_dlock.is_locked("exclusive-task") is False


@pytest.mark.asyncio
async def test_coordinator_extend_interval_refreshes_ttl(
    redis_dlock: RedisDistributedLockAdapter,
) -> None:
    coord = DistributedLockScope(
        cmd=redis_dlock,
        owner_provider=lambda: "extend-owner",
        extend_interval=timedelta(milliseconds=200),
        wait_timeout=timedelta(seconds=2),
    )

    async with coord.scope("refresh-key") as held:
        token_at_entry = held.token
        await asyncio.sleep(0.45)
        ttl = await redis_dlock.get_ttl("refresh-key")
        assert ttl is not None
        # Extend resets the lease; TTL should stay near the 60s spec, not decay with wall clock.
        assert ttl.total_seconds() >= 59.0
        # The heartbeat extends the same lock generation: the token is unchanged
        # (re-acquiring would have bumped the per-key fencing counter).
        assert held.token == token_at_entry

    # reset never bumps the counter: the next generation is exactly +1.
    again = await redis_dlock.acquire("refresh-key", "next-owner")
    assert again is not None
    assert again.token == (token_at_entry or 0) + 1
