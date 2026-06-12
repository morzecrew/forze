"""Integration: per-entry TTL + sliding expiration on the Redis cache adapter."""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from forze_redis.adapters.cache import RedisCacheAdapter
from forze_redis.kernel.client import RedisClient

# ----------------------- #


def _adapter(client: RedisClient, **kw: object) -> RedisCacheAdapter:
    return RedisCacheAdapter(
        client=client,
        namespace=f"it:adttl:{uuid4().hex[:12]}",
        ttl_pointer=timedelta(seconds=60),
        ttl_body=timedelta(seconds=300),
        **kw,  # type: ignore[arg-type]
    )


def _pointer_key(adapter: RedisCacheAdapter, key: str) -> str:
    return adapter.construct_key(("cache", "pointer"), key)


def _body_key(adapter: RedisCacheAdapter, key: str, version: str) -> str:
    return adapter.construct_key(("cache", "body"), key, version)


class TestPerEntryTtl:
    async def test_ttl_overrides_pointer_and_body(
        self, redis_client: RedisClient
    ) -> None:
        adapter = _adapter(redis_client)

        await adapter.set_versioned("pk", "1", {"x": 1}, ttl=timedelta(seconds=1800))

        pointer_ms = await redis_client.pttl_raw_ms(_pointer_key(adapter, "pk"))
        body_ms = await redis_client.pttl_raw_ms(_body_key(adapter, "pk", "1"))

        assert 1700_000 < pointer_ms <= 1800_000
        assert 1700_000 < body_ms <= 1800_000

    async def test_default_ttls_without_override(
        self, redis_client: RedisClient
    ) -> None:
        adapter = _adapter(redis_client)

        await adapter.set_versioned("pk", "1", {"x": 1})

        pointer_ms = await redis_client.pttl_raw_ms(_pointer_key(adapter, "pk"))
        body_ms = await redis_client.pttl_raw_ms(_body_key(adapter, "pk", "1"))

        assert 50_000 < pointer_ms <= 60_000
        assert 290_000 < body_ms <= 300_000


class TestSlidingExpiration:
    async def test_hit_extends_pointer_not_body(
        self, redis_client: RedisClient
    ) -> None:
        adapter = _adapter(redis_client, sliding_ttl=timedelta(seconds=120))

        await adapter.set_versioned("pk", "1", {"x": 1})  # pointer 60s, body 300s

        assert await adapter.get("pk") is not None

        pointer_ms = await redis_client.pttl_raw_ms(_pointer_key(adapter, "pk"))
        body_ms = await redis_client.pttl_raw_ms(_body_key(adapter, "pk", "1"))

        assert pointer_ms > 110_000  # extended to the 120s window
        assert body_ms <= 300_000  # cap untouched

    async def test_extension_never_shortens(self, redis_client: RedisClient) -> None:
        # Age-stretched entry: pointer already lives longer than the window.
        adapter = _adapter(redis_client, sliding_ttl=timedelta(seconds=120))

        await adapter.set_versioned("pk", "1", {"x": 1}, ttl=timedelta(seconds=1800))
        assert await adapter.get("pk") is not None

        pointer_ms = await redis_client.pttl_raw_ms(_pointer_key(adapter, "pk"))
        assert pointer_ms > 1700_000  # EXPIRE GT left the longer TTL alone

    async def test_get_many_extends_hits(self, redis_client: RedisClient) -> None:
        adapter = _adapter(redis_client, sliding_ttl=timedelta(seconds=120))

        await adapter.set_many_versioned({("a", "1"): {"x": 1}, ("b", "1"): {"x": 2}})

        hits, misses = await adapter.get_many(["a", "b", "missing"])
        assert set(hits) == {"a", "b"}
        assert misses == ["missing"]

        for key in ("a", "b"):
            pointer_ms = await redis_client.pttl_raw_ms(_pointer_key(adapter, key))
            assert pointer_ms > 110_000
