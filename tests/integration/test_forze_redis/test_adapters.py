from datetime import timedelta
from uuid import uuid4

import pytest

from forze.application.contracts.idempotency import IdempotencySnapshot
from forze.base.errors import ConflictError
from forze_redis.adapters.cache import RedisCacheAdapter
from forze_redis.adapters.codecs import RedisKeyCodec
from forze_redis.adapters.counter import RedisCounterAdapter
from forze_redis.adapters.idempotency import RedisIdempotencyAdapter
from forze_redis.kernel.platform.client import RedisClient


@pytest.mark.asyncio
async def test_redis_cache_adapter_roundtrip(redis_client: RedisClient) -> None:
    namespace = f"it:redis-cache:{uuid4()}"
    cache = RedisCacheAdapter(
        client=redis_client, key_codec=RedisKeyCodec(namespace=namespace)
    )

    await cache.set("plain", {"name": "plain"})
    await cache.set_versioned("doc", "v1", {"name": "old"})
    await cache.set_versioned("doc", "v2", {"name": "new"})

    assert await cache.get("plain") == {"name": "plain"}
    assert await cache.get("doc") == {"name": "new"}

    hits, misses = await cache.get_many(["plain", "doc", "missing"])
    assert hits == {"plain": {"name": "plain"}, "doc": {"name": "new"}}
    assert misses == ["missing"]

    await cache.delete("doc", hard=False)
    assert await cache.get("doc") is None


@pytest.mark.asyncio
async def test_redis_counter_adapter_operations(redis_client: RedisClient) -> None:
    counter = RedisCounterAdapter(
        client=redis_client,
        key_codec=RedisKeyCodec(namespace=f"it:redis-counter:{uuid4()}"),
    )

    assert await counter.incr() == 1
    assert await counter.incr_batch(size=3) == [2, 3, 4]
    assert await counter.decr(by=2) == 2
    assert await counter.reset(value=10) == 2
    assert await counter.incr() == 11


@pytest.mark.asyncio
async def test_redis_idempotency_adapter_replays_snapshot(
    redis_client: RedisClient,
) -> None:
    adapter = RedisIdempotencyAdapter(
        client=redis_client,
        ttl=timedelta(seconds=30),
        key_codec=RedisKeyCodec(namespace=f"it:redis-idempotency:{uuid4()}"),
    )
    op = f"orders:{uuid4()}"
    key = f"request:{uuid4()}"
    payload_hash = "hash-1"

    assert await adapter.begin(op, key, payload_hash) is None

    with pytest.raises(ConflictError, match="pending"):
        await adapter.begin(op, key, payload_hash)

    snapshot = IdempotencySnapshot(
        code=201,
        content_type="application/json",
        body=b'{"id":"1"}',
    )
    await adapter.commit(op, key, payload_hash, snapshot)

    replay = await adapter.begin(op, key, payload_hash)
    assert replay is not None
    assert replay.code == 201
    assert replay.content_type == "application/json"
    assert replay.body == b'{"id":"1"}'

    with pytest.raises(ConflictError, match="Payload hash mismatch"):
        await adapter.begin(op, key, "hash-2")
