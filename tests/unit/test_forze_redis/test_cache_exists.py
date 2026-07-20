"""RedisCacheAdapter.exists: presence without payload transfer."""

from __future__ import annotations

from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import asynccontextmanager

import attrs

from forze_redis.adapters.cache import RedisCacheAdapter

# ----------------------- #


@attrs.define
class FakeRedisClient:
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
            self.store[key] = (
                value if isinstance(value, bytes) else value.encode("utf-8")
            )

        return True

    async def unlink(self, *keys: str) -> int:
        return sum(1 for k in keys if self.store.pop(k, None) is not None)

    async def exists(self, key: str) -> bool:
        return key in self.store

    @asynccontextmanager
    async def pipeline(
        self, *, transaction: bool = True
    ) -> AsyncGenerator[FakeRedisClient]:
        yield self


def _adapter(client: FakeRedisClient) -> RedisCacheAdapter:
    return RedisCacheAdapter(client=client, namespace="app:products")  # type: ignore[arg-type]


# ----------------------- #


class TestExists:
    async def test_versioned_entry_exists(self) -> None:
        client = FakeRedisClient()
        adapter = _adapter(client)

        await adapter.set_versioned("pk", "1", {"x": 1})

        assert await adapter.exists("pk") is True

    async def test_plain_kv_entry_exists(self) -> None:
        client = FakeRedisClient()
        adapter = _adapter(client)

        await adapter.set("pk", {"x": 1})

        assert await adapter.exists("pk") is True

    async def test_missing_key(self) -> None:
        adapter = _adapter(FakeRedisClient())

        assert await adapter.exists("missing") is False

    async def test_dangling_pointer_without_body_falls_to_kv(self) -> None:
        client = FakeRedisClient()
        adapter = _adapter(client)

        await adapter.set_versioned("pk", "1", {"x": 1})
        # The body expired/evicted while the pointer survived.
        body_key = adapter.construct_key(("cache", "body"), "pk", "1")
        del client.store[body_key]

        assert await adapter.exists("pk") is False  # mirrors get()'s miss

    async def test_hard_delete_clears_presence(self) -> None:
        client = FakeRedisClient()
        adapter = _adapter(client)

        await adapter.set_versioned("pk", "1", {"x": 1})
        await adapter.delete("pk", hard=True)

        assert await adapter.exists("pk") is False
