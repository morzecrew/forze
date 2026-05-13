"""Unit tests for ``forze_redis.execution.deps`` (module and configurable factories)."""

from datetime import timedelta
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("redis")

from forze.application.contracts.cache import CacheDepKey, CacheSpec
from forze.application.contracts.counter import CounterDepKey, CounterSpec
from forze.application.contracts.dlock import (
    DistributedLockCommandDepKey,
    DistributedLockQueryDepKey,
    DistributedLockSpec,
)
from forze.application.contracts.idempotency import IdempotencyDepKey, IdempotencySpec
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import CallContext, Deps, ExecutionContext
from forze_redis.adapters import (
    RedisCacheAdapter,
    RedisCounterAdapter,
    RedisDistributedLockAdapter,
    RedisIdempotencyAdapter,
)
from forze_redis.execution.deps.deps import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisDistributedLock,
    ConfigurableRedisIdempotency,
)
from forze_redis.execution.deps.keys import RedisBlockingClientDepKey, RedisClientDepKey
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
            dlocks={"dl1": {"namespace": "dlock1"}},
        )

        deps = module()

        assert isinstance(deps, Deps)
        assert deps.exists(RedisClientDepKey)
        assert deps.exists(CacheDepKey, route="c1")
        assert deps.exists(CounterDepKey, route="n1")
        assert deps.exists(IdempotencyDepKey, route="idem1")
        assert deps.exists(DistributedLockQueryDepKey, route="dl1")
        assert deps.exists(DistributedLockCommandDepKey, route="dl1")

    def test_registers_optional_blocking_client(self) -> None:
        main = MagicMock(spec=RedisClient)
        blocking = MagicMock(spec=RedisClient)
        module = RedisDepsModule(client=main, blocking_client=blocking)

        deps = module()

        assert deps.provide(RedisClientDepKey) is main
        assert deps.provide(RedisBlockingClientDepKey) is blocking
    def test_cache_adapter(self) -> None:
        factory = ConfigurableRedisCache(
            config={"namespace": "acme", "tenant_aware": True},
        )
        ctx = _ctx()
        tid = uuid4()
        with ctx.bind_call(
            call=CallContext(execution_id=uuid4(), correlation_id=uuid4()),
            identity=AuthnIdentity(principal_id=uuid4()),
            tenancy=TenantIdentity(tenant_id=tid),
        ):
            spec = CacheSpec(name="cache")
            adapter = factory(ctx, spec)
            assert adapter.tenant_provider().tenant_id == tid

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

    def test_distributed_lock_adapter(self) -> None:
        factory = ConfigurableRedisDistributedLock(config={"namespace": "dl"})
        ctx = _ctx()
        spec = DistributedLockSpec(name="dl", ttl=timedelta(seconds=30))
        cmd = factory(ctx, spec)
        query = factory(ctx, spec)

        assert isinstance(cmd, RedisDistributedLockAdapter)
        assert isinstance(query, RedisDistributedLockAdapter)
        assert cmd.spec is spec
        assert cmd.tenant_aware is False

        deps = RedisDepsModule(client=MagicMock(spec=RedisClient), dlocks={"dl": {"namespace": "x"}})()
        ctx2 = ExecutionContext(deps=deps)
        q = ctx2.dlock_query(spec)
        c = ctx2.dlock_command(spec)
        assert isinstance(q, RedisDistributedLockAdapter)
        assert isinstance(c, RedisDistributedLockAdapter)
