import uuid

import attrs

from forze_redis.adapters.cache import RedisCacheAdapter
from forze_redis.adapters.codecs import RedisKeyCodec


@attrs.define
class MockRedisClient:
    pass


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
        tenant_provider=lambda: tenant_id,
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
