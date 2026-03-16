from pydantic import BaseModel
from unittest.mock import Mock

from forze.application.contracts.pubsub import PubSubSpec
from forze.application.contracts.tenant import TenantContextDepKey, TenantContextPort
from forze.application.execution import ExecutionContext, Deps
from forze.base.codecs import KeyCodec
from forze_redis.adapters.counter import RedisCounterAdapter
from forze_redis.adapters.pubsub import RedisPubSubAdapter
from forze_redis.execution.deps.deps import redis_counter, redis_pubsub
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.kernel.platform.client import RedisClient


def test_redis_counter_without_tenant_context():
    redis_mock = Mock(spec=RedisClient)
    deps = Deps(deps={RedisClientDepKey: redis_mock})
    context = ExecutionContext(deps=deps)

    counter = redis_counter(context, namespace="test-namespace")

    assert isinstance(counter, RedisCounterAdapter)
    assert counter.client is redis_mock
    assert counter.key_codec == KeyCodec(namespace="test-namespace")
    assert counter.tenant_context is None


def test_redis_counter_with_tenant_context():
    redis_mock = Mock(spec=RedisClient)
    tenant_mock = Mock(spec=TenantContextPort)

    deps = Deps(
        deps={RedisClientDepKey: redis_mock, TenantContextDepKey: lambda: tenant_mock}
    )
    context = ExecutionContext(deps=deps)

    counter = redis_counter(context, namespace="test-namespace")

    assert isinstance(counter, RedisCounterAdapter)
    assert counter.client is redis_mock
    assert counter.key_codec == KeyCodec(namespace="test-namespace")
    assert counter.tenant_context is tenant_mock


class _PubSubPayload(BaseModel):
    value: str


def test_redis_pubsub_builds_adapter():
    redis_mock = Mock(spec=RedisClient)
    deps = Deps(deps={RedisClientDepKey: redis_mock})
    context = ExecutionContext(deps=deps)
    spec = PubSubSpec(namespace="events", model=_PubSubPayload)

    pubsub = redis_pubsub(context, spec)

    assert isinstance(pubsub, RedisPubSubAdapter)
    assert pubsub.client is redis_mock
    assert pubsub.codec.model is _PubSubPayload
