"""Integration tests for invalidation push (CLIENT TRACKING, RESP3 push frames).

Real Redis: a subscriber on client A must observe invalidations caused by
writes performed through client B — the cross-replica scenario the document
L1's v2 exists for.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest_asyncio

from forze.application.contracts.cache import CacheInvalidation
from forze_redis.adapters.cache import RedisCacheAdapter
from forze_redis.kernel.client import RedisClient

# ----------------------- #


@pytest_asyncio.fixture(scope="function")
async def second_client(redis_container) -> RedisClient:  # type: ignore[no-untyped-def]
    """A second, independent client — the 'other replica'."""

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)

    client = RedisClient()
    await client.initialize(dsn=f"redis://{host}:{port}/0")

    yield client

    await client.close()


def _adapter(client: RedisClient, namespace: str) -> RedisCacheAdapter:
    return RedisCacheAdapter(
        client=client,
        namespace=namespace,
        invalidation_push=True,
    )


async def _wait_for(predicate, *, timeout: float = 5.0) -> None:  # type: ignore[no-untyped-def]
    deadline = asyncio.get_running_loop().time() + timeout

    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("Timed out waiting for invalidation event")

        await asyncio.sleep(0.05)


class TestInvalidationPush:
    async def test_cross_client_write_pushes_invalidation(
        self,
        redis_client: RedisClient,
        second_client: RedisClient,
    ) -> None:
        namespace = f"it:l1push:{uuid4().hex[:12]}"
        subscriber = _adapter(redis_client, namespace)
        writer = _adapter(second_client, namespace)

        events: list[CacheInvalidation] = []
        unsubscribe = await subscriber.subscribe_invalidations(events.append)
        assert unsubscribe is not None

        try:
            # The hub flushes on (re)connect: wait for the initial reset so the
            # subsequent key event is unambiguous.
            await _wait_for(lambda: any(e.key is None for e in events))

            await writer.set_versioned("pk-1", "1", {"hello": "world"})

            await _wait_for(lambda: any(e.key == "pk-1" for e in events))

        finally:
            await unsubscribe()

        # The last unsubscribe winds the hub down: its listener task exits and
        # the pinned connection is released back to the pool.
        hub = redis_client._RedisClient__invalidation_hub
        assert hub is not None
        await _wait_for(lambda: hub._task is None or hub._task.done())

    async def test_delete_pushes_invalidation(
        self,
        redis_client: RedisClient,
        second_client: RedisClient,
    ) -> None:
        namespace = f"it:l1push:{uuid4().hex[:12]}"
        subscriber = _adapter(redis_client, namespace)
        writer = _adapter(second_client, namespace)

        await writer.set_versioned("pk-2", "1", {"hello": "world"})

        events: list[CacheInvalidation] = []
        unsubscribe = await subscriber.subscribe_invalidations(events.append)
        assert unsubscribe is not None

        try:
            await _wait_for(lambda: any(e.key is None for e in events))

            await writer.delete("pk-2", hard=True)

            await _wait_for(lambda: any(e.key == "pk-2" for e in events))

        finally:
            await unsubscribe()

    async def test_other_namespace_writes_not_delivered(
        self,
        redis_client: RedisClient,
        second_client: RedisClient,
    ) -> None:
        namespace = f"it:l1push:{uuid4().hex[:12]}"
        other = f"it:l1push:{uuid4().hex[:12]}"
        subscriber = _adapter(redis_client, namespace)
        writer_other = _adapter(second_client, other)
        writer_same = _adapter(second_client, namespace)

        events: list[CacheInvalidation] = []
        unsubscribe = await subscriber.subscribe_invalidations(events.append)
        assert unsubscribe is not None

        try:
            await _wait_for(lambda: any(e.key is None for e in events))

            await writer_other.set_versioned("pk-other", "1", {"x": 1})
            await writer_same.set_versioned("pk-same", "1", {"x": 1})

            await _wait_for(lambda: any(e.key == "pk-same" for e in events))
            assert not any(e.key == "pk-other" for e in events)

        finally:
            await unsubscribe()
