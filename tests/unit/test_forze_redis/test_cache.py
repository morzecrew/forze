from forze_redis.adapters.cache import RedisCacheAdapter
from forze.base.codecs import KeyCodec

import attrs
import uuid


@attrs.define
class MockTenantContext:
    _id: uuid.UUID

    def get(self) -> uuid.UUID:
        return self._id


class MockRedisClient:
    pass


def test_redis_cache_adapter_keys_no_tenant():
    client = MockRedisClient()
    key_codec = KeyCodec(namespace="test")

    adapter = RedisCacheAdapter(
        client=client,  # type: ignore
        key_codec=key_codec,
    )

    # Test internal key generation methods
    assert adapter._RedisCacheAdapter__kv_key("mykey") == "test:cache:kv:mykey"
    assert (
        adapter._RedisCacheAdapter__pointer_key("mykey") == "test:cache:pointer:mykey"
    )
    assert (
        adapter._RedisCacheAdapter__body_key("mykey", "v1")
        == "test:cache:body:mykey:v1"
    )


def test_redis_cache_adapter_keys_with_tenant():
    tenant_id = uuid.uuid4()
    tenant_context = MockTenantContext(tenant_id)

    client = MockRedisClient()
    key_codec = KeyCodec(namespace="test")

    adapter = RedisCacheAdapter(
        client=client,  # type: ignore
        key_codec=key_codec,
        tenant_context=tenant_context,
    )

    # Test internal key generation methods
    assert (
        adapter._RedisCacheAdapter__kv_key("mykey")
        == f"test:{tenant_id}:cache:kv:mykey"
    )
    assert (
        adapter._RedisCacheAdapter__pointer_key("mykey")
        == f"test:{tenant_id}:cache:pointer:mykey"
    )
    assert (
        adapter._RedisCacheAdapter__body_key("mykey", "v1")
        == f"test:{tenant_id}:cache:body:mykey:v1"
    )
