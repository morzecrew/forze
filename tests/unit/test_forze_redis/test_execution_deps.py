"""Unit tests for ``forze_redis.execution.deps`` (module and configurable factories)."""

from datetime import timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.cache import CacheDepKey, CacheSpec
from forze.application.contracts.counter import CounterDepKey, CounterSpec
from forze.application.contracts.idempotency import IdempotencyDepKey, IdempotencySpec
from forze.application.execution import CallContext, Deps, ExecutionContext, PrincipalContext
from forze_redis.adapters import RedisCacheAdapter, RedisCounterAdapter, RedisIdempotencyAdapter
from forze_redis.execution.deps.deps import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisIdempotency,
)
from forze_redis.execution.deps.keys import RedisClientDepKey
from forze_redis.execution.deps.module import RedisDepsModule
from forze_redis.kernel.platform import RedisClient


def _ctx() -> ExecutionContext:
    return ExecutionContext(deps=Deps.plain({RedisClientDepKey: MagicMock(spec=RedisClient)}))


class TestRedisDepsModule:
    def test_registers_client_and_routed_ports(self) -> None:
        client = MagicMock(spec=RedisClient)
        module = RedisDepsModule(
            client=client,
            caches={"c1": {"namespace": "ns1"}},
            counters={"n1": {"namespace": "ctr1"}},
            idempotency={"idem1": {"namespace": "id1"}},
        )

        deps = module()

        assert isinstance(deps, Deps)
        assert deps.exists(RedisClientDepKey)
        assert deps.exists(CacheDepKey, route="c1")
        assert deps.exists(CounterDepKey, route="n1")
        assert deps.exists(IdempotencyDepKey, route="idem1")


class TestConfigurableRedisFactories:
    def test_cache_adapter(self) -> None:
        factory = ConfigurableRedisCache(
            config={"namespace": "acme", "tenant_aware": True},
        )
        ctx = _ctx()
        tid = uuid4()
        with ctx.bind_call(
            call=CallContext(execution_id=uuid4(), correlation_id=uuid4()),
            principal=PrincipalContext(tenant_id=tid),
        ):
            spec = CacheSpec(name="cache")
            adapter = factory(ctx, spec)
            assert adapter.tenant_provider() == tid

        assert isinstance(adapter, RedisCacheAdapter)
        assert adapter.tenant_aware is True
        assert adapter.ttl_pointer == spec.ttl_pointer
        assert adapter.ttl_body == spec.ttl
        assert adapter.ttl_kv == spec.ttl

    def test_counter_adapter(self) -> None:
        factory = ConfigurableRedisCounter(config={"namespace": "ctr"})
        ctx = _ctx()
        adapter = factory(ctx, CounterSpec(name="c"))

        assert isinstance(adapter, RedisCounterAdapter)
        assert adapter.tenant_aware is False

    def test_idempotency_adapter(self) -> None:
        factory = ConfigurableRedisIdempotency(config={"namespace": "idem"})
        ctx = _ctx()
        adapter = factory(
            ctx,
            IdempotencySpec(name="i", ttl=timedelta(minutes=5)),
        )

        assert isinstance(adapter, RedisIdempotencyAdapter)
        assert adapter.ttl == timedelta(minutes=5)
