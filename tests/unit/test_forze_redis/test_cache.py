import uuid
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager

import attrs

from forze.base.codecs import JsonCodec

from forze.application.contracts.tenancy import TenantIdentity

from forze_redis.adapters.cache import RedisCacheAdapter
from forze_redis.adapters.codecs import RedisKeyCodec


@attrs.define
class MockRedisClient:
    pass


@attrs.define
class FakeRedisClient:
    """In-memory async Redis stub for :class:`RedisCacheAdapter` unit tests."""

    store: dict[str, bytes] = attrs.Factory(dict)

    async def mget(self, keys: Sequence[str]) -> list[bytes | None]:
        return [self.store.get(k) for k in keys]

    async def mset(
        self,
        mapping: Mapping[str, bytes | str],
        *,
        ex: int | None = None,
        px: int | None = None,
        nx: bool = False,
        xx: bool = False,
    ) -> bool:
        for key, value in mapping.items():
            self.store[key] = value if isinstance(value, bytes) else value.encode("utf-8")

        return True

    async def unlink(self, *keys: str) -> int:
        removed = 0

        for key in keys:
            if key in self.store:
                del self.store[key]
                removed += 1

        return removed

    @asynccontextmanager
    async def pipeline(self, *, transaction: bool = True) -> AsyncIterator["FakeRedisClient"]:
        yield self


def test_redis_cache_adapter_keys_no_tenant() -> None:
    client = MockRedisClient()
    key_codec = RedisKeyCodec(namespace="test")

    adapter = RedisCacheAdapter(
        client=client,  # type: ignore[arg-type]
        key_codec=key_codec,
    )

    assert adapter._RedisCacheAdapter__kv_key("mykey") == "cache:kv:test:mykey"
    assert adapter._RedisCacheAdapter__pointer_key("mykey") == "cache:pointer:test:mykey"
    assert (
        adapter._RedisCacheAdapter__body_key("mykey", "v1") == "cache:body:test:mykey:v1"
    )


def test_redis_cache_adapter_keys_with_tenant() -> None:
    tenant_id = uuid.uuid4()
    client = MockRedisClient()
    key_codec = RedisKeyCodec(namespace="test")

    adapter = RedisCacheAdapter(
        client=client,  # type: ignore[arg-type]
        key_codec=key_codec,
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant_id),
    )

    tid = str(tenant_id)
    assert (
        adapter._RedisCacheAdapter__kv_key("mykey")
        == f"tenant:{tid}:cache:kv:test:mykey"
    )
    assert (
        adapter._RedisCacheAdapter__pointer_key("mykey")
        == f"tenant:{tid}:cache:pointer:test:mykey"
    )
    assert (
        adapter._RedisCacheAdapter__body_key("mykey", "v1")
        == f"tenant:{tid}:cache:body:test:mykey:v1"
    )


# ....................... #


def _kv(adapter: RedisCacheAdapter, logical: str) -> str:
    return adapter._RedisCacheAdapter__kv_key(logical)


def _pointer(adapter: RedisCacheAdapter, logical: str) -> str:
    return adapter._RedisCacheAdapter__pointer_key(logical)


def _body(adapter: RedisCacheAdapter, logical: str, version: str) -> str:
    return adapter._RedisCacheAdapter__body_key(logical, version)


async def test_cache_get_plain_round_trip() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    await adapter.set("a", {"x": 1})
    assert await adapter.get("a") == {"x": 1}


async def test_cache_get_versioned_round_trip() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    await adapter.set_versioned("v", "1", [1, 2])
    assert await adapter.get("v") == [1, 2]


async def test_cache_get_miss_returns_none() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    assert await adapter.get("nope") is None


async def test_cache_get_orphan_pointer_falls_back_to_plain_kv() -> None:
    """Pointer without body JSON should not block plain KV reads."""
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    fake.store[_pointer(adapter, "k")] = b"v1"
    fake.store[_kv(adapter, "k")] = JsonCodec().dumps({"plain": True})

    assert await adapter.get("k") == {"plain": True}


async def test_cache_get_many_empty_keys() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    hits, misses = await adapter.get_many([])
    assert hits == {}
    assert misses == []


async def test_cache_get_many_skips_corrupt_kv_entries() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    fake.store[_kv(adapter, "good")] = JsonCodec().dumps(1)
    fake.store[_kv(adapter, "bad")] = b"not-json"

    hits, misses = await adapter.get_many(["good", "bad", "missing"])
    assert hits == {"good": 1}
    assert set(misses) == {"bad", "missing"}


async def test_cache_get_many_large_batch_uses_thread_path() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    keys = [f"k{i}" for i in range(65)]
    for i, logical in enumerate(keys):
        fake.store[_kv(adapter, logical)] = JsonCodec().dumps(i)

    hits, misses = await adapter.get_many(keys)
    assert misses == []
    assert hits == {logical: idx for idx, logical in enumerate(keys)}


async def test_cache_set_many_and_set_many_versioned_noop_for_empty() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    await adapter.set_many({})
    await adapter.set_many_versioned({})
    assert fake.store == {}


async def test_cache_delete_soft_and_hard() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    await adapter.set("plain", 1)
    await adapter.delete("plain", hard=False)
    assert await adapter.get("plain") is None

    await adapter.set_versioned("ver", "a", {"z": 3})
    await adapter.delete("ver", hard=True)
    assert await adapter.get("ver") is None
    assert _pointer(adapter, "ver") not in fake.store
    assert _body(adapter, "ver", "a") not in fake.store


async def test_cache_delete_hard_without_pointer_still_unlinks() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    await adapter.set("only_kv", 9)
    await adapter.delete("only_kv", hard=True)
    assert await adapter.get("only_kv") is None


async def test_cache_delete_many_empty_is_noop() -> None:
    fake = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=fake,  # type: ignore[arg-type]
        key_codec=RedisKeyCodec(namespace="ns"),
    )

    await adapter.delete_many([], hard=True)
    assert fake.store == {}
