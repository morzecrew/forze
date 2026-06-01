"""Redis cache adapter bytes passthrough."""

from datetime import timedelta

import pytest

from forze_redis.adapters.cache import RedisCacheAdapter

from .test_cache import FakeRedisClient


@pytest.mark.asyncio
async def test_mset_bodies_stores_bytes_without_json_wrap() -> None:
    client = FakeRedisClient()
    adapter = RedisCacheAdapter(
        client=client,  # type: ignore[arg-type]
        namespace="t",
    )
    raw = b'{"id":"1","rev":1}'

    await adapter._RedisCacheAdapter__mset_bodies(  # type: ignore[attr-defined]
        {("doc", "1"): raw},
        ttl=timedelta(seconds=60),
    )

    assert len(client.store) == 1
    stored = next(iter(client.store.values()))
    assert stored == raw
