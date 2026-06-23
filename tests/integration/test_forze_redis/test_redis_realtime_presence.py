"""Real-Redis presence with TTL: counts prune lapsed members, a heartbeat keeps a
member live, and a clean ``left`` removes it — the crash-safe multi-node presence.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze_redis.adapters import RedisRealtimePresence
from forze_redis.kernel.client import RedisClient

pytestmark = pytest.mark.integration


def _presence(redis_client: RedisClient, *, ttl: timedelta) -> RedisRealtimePresence:
    return RedisRealtimePresence(
        client=redis_client, namespace=f"it:{uuid4().hex[:10]}", ttl=ttl
    )


@pytest.mark.asyncio
async def test_join_count_and_leave(redis_client: RedisClient) -> None:
    presence = _presence(redis_client, ttl=timedelta(seconds=60))
    room = "t:acme:principal:u1"

    await presence.joined(room, "sid-a")
    await presence.joined(room, "sid-b")
    assert await presence.count(room) == 2

    await presence.left(room, "sid-a")
    assert await presence.count(room) == 1
    assert await presence.count("t:acme:principal:nobody") == 0


@pytest.mark.asyncio
async def test_member_expires_without_heartbeat(redis_client: RedisClient) -> None:
    presence = _presence(redis_client, ttl=timedelta(milliseconds=300))
    room = "t:acme:principal:u2"

    await presence.joined(room, "sid-a")
    assert await presence.count(room) == 1

    await asyncio.sleep(0.5)  # past the TTL with no refresh
    assert await presence.count(room) == 0  # lapsed member is pruned on count


@pytest.mark.asyncio
async def test_heartbeat_keeps_member_live(redis_client: RedisClient) -> None:
    presence = _presence(redis_client, ttl=timedelta(milliseconds=400))
    room = "t:acme:principal:u3"

    await presence.joined(room, "sid-a")
    for _ in range(4):  # refresh within the TTL, total elapsed > one TTL
        await asyncio.sleep(0.2)
        await presence.joined(room, "sid-a")  # heartbeat

    assert await presence.count(room) == 1  # never lapsed
