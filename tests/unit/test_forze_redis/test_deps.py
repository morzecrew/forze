from unittest.mock import Mock
import pytest

from forze.application.contracts.tenant import TenantContextDepKey, TenantContextPort
from forze.application.execution import ExecutionContext, Deps
from forze.utils.codecs import KeyCodec
from forze_redis.adapters.counter import RedisCounterAdapter
from forze_redis.execution.deps.deps import redis_counter
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

    deps = Deps(deps={
        RedisClientDepKey: redis_mock,
        TenantContextDepKey: lambda: tenant_mock
    })
    context = ExecutionContext(deps=deps)

    counter = redis_counter(context, namespace="test-namespace")

    assert isinstance(counter, RedisCounterAdapter)
    assert counter.client is redis_mock
    assert counter.key_codec == KeyCodec(namespace="test-namespace")
    assert counter.tenant_context is tenant_mock
