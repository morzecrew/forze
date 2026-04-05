from unittest.mock import Mock
from uuid import uuid4

from pydantic import BaseModel

from forze.application.contracts.counter import CounterSpec
from forze.application.contracts.pubsub import PubSubSpec
from forze.application.execution import Deps, ExecutionContext
from forze_redis.adapters import RedisPubSubAdapter, RedisPubSubCodec
from forze_redis.adapters.counter import RedisCounterAdapter
from forze_redis.adapters.codecs import RedisKeyCodec
from forze_redis.execution.deps.deps import ConfigurableRedisCounter
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.kernel.platform.client import RedisClient


def test_redis_counter_factory_builds_adapter() -> None:
    redis_mock = Mock(spec=RedisClient)
    deps = Deps.plain({RedisClientDepKey: redis_mock})
    context = ExecutionContext(deps=deps)

    factory = ConfigurableRedisCounter(config={"namespace": "test-namespace"})
    counter = factory(context, CounterSpec(name="test-namespace"))

    assert isinstance(counter, RedisCounterAdapter)
    assert counter.client is redis_mock
    assert counter.key_codec == RedisKeyCodec(namespace="test-namespace")
    assert counter.tenant_aware is False


def test_redis_counter_factory_tenant_aware_uses_context() -> None:
    redis_mock = Mock(spec=RedisClient)
    deps = Deps.plain({RedisClientDepKey: redis_mock})
    context = ExecutionContext(deps=deps)
    tid = uuid4()

    factory = ConfigurableRedisCounter(
        config={"namespace": "ns", "tenant_aware": True},
    )
    counter = factory(context, CounterSpec(name="ns"))

    from forze.application.execution import CallContext, PrincipalContext

    call = CallContext(execution_id=uuid4(), correlation_id=uuid4())
    principal = PrincipalContext(tenant_id=tid)

    with context.bind_call(call=call, principal=principal):
        assert counter.tenant_provider() == tid


class _PubSubPayload(BaseModel):
    value: str


def test_redis_pubsub_adapter_constructible() -> None:
    redis_mock = Mock(spec=RedisClient)
    codec = RedisPubSubCodec(model=_PubSubPayload)
    adapter = RedisPubSubAdapter(client=redis_mock, codec=codec)

    assert adapter.client is redis_mock
    assert adapter.codec.model is _PubSubPayload
