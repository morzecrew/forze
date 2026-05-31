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
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.application.contracts.idempotency import IdempotencyDepKey, IdempotencySpec
from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze_redis.adapters import (
    RedisCacheAdapter,
    RedisCounterAdapter,
    RedisDistributedLockAdapter,
    RedisIdempotencyAdapter,
)
from forze_redis.execution.deps.configs import (
    RedisCacheConfig,
    RedisCounterConfig,
    RedisDistributedLockConfig,
    RedisIdempotencyConfig,
)
from forze_redis.execution.deps import (
    ConfigurableRedisCache,
    ConfigurableRedisCounter,
    ConfigurableRedisDistributedLock,
    ConfigurableRedisIdempotency,
)
from forze_redis.execution.deps.keys import RedisBlockingClientDepKey, RedisClientDepKey
from forze_redis.execution.deps.module import RedisDepsModule
from forze_redis.kernel.client import RedisClient


def _ctx() -> ExecutionContext:
    return context_from_deps(Deps.plain({RedisClientDepKey: MagicMock(spec=RedisClient)}))


class TestConfigurableRedisCache:
    def test_rejects_mapping_config(self) -> None:
        with pytest.raises(TypeError, match="RedisUniversalConfig"):
            ConfigurableRedisCache(config={"namespace": "acme"})


class TestRedisDepsModule:
    def test_registers_client_and_routed_ports(self) -> None:
        client = MagicMock(spec=RedisClient)
        module = RedisDepsModule(
            client=client,
            caches={"c1": RedisCacheConfig(namespace="ns1")},
            counters={"n1": RedisCounterConfig(namespace="ctr1")},
            idempotency={"idem1": RedisIdempotencyConfig(namespace="id1")},
            dlocks={"dl1": RedisDistributedLockConfig(namespace="dlock1")},
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

        resolved = frozen_deps_from_deps(module())

        assert resolved.provide(RedisClientDepKey) is main
        assert resolved.provide(RedisBlockingClientDepKey) is blocking
    def test_cache_adapter(self) -> None:
        factory = ConfigurableRedisCache(
            config=RedisCacheConfig(namespace="acme", tenant_aware=True),
        )
        ctx = _ctx()
        tid = uuid4()
        with ctx.inv_ctx.bind(
            metadata=InvocationMetadata(
                execution_id=uuid4(),
                correlation_id=uuid4(),
            ),
            authn=AuthnIdentity(principal_id=uuid4()),
            tenant=TenantIdentity(tenant_id=tid),
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
        factory = ConfigurableRedisCounter(
            config=RedisCounterConfig(namespace="ctr"),
        )
        ctx = _ctx()
        adapter = factory(ctx, CounterSpec(name="c"))

        assert isinstance(adapter, RedisCounterAdapter)
        assert adapter.tenant_aware is False

    def test_idempotency_adapter(self) -> None:
        factory = ConfigurableRedisIdempotency(
            config=RedisIdempotencyConfig(namespace="idem"),
        )
        ctx = _ctx()
        adapter = factory(
            ctx,
            IdempotencySpec(name="i", ttl=timedelta(minutes=5)),
        )

        assert isinstance(adapter, RedisIdempotencyAdapter)
        assert adapter.ttl == timedelta(minutes=5)

    def test_distributed_lock_adapter(self) -> None:
        factory = ConfigurableRedisDistributedLock(
            config=RedisDistributedLockConfig(namespace="dl"),
        )
        ctx = _ctx()
        spec = DistributedLockSpec(name="dl", ttl=timedelta(seconds=30))
        cmd = factory(ctx, spec)
        query = factory(ctx, spec)

        assert isinstance(cmd, RedisDistributedLockAdapter)
        assert isinstance(query, RedisDistributedLockAdapter)
        assert cmd.spec is spec
        assert cmd.tenant_aware is False

        deps = RedisDepsModule(
            client=MagicMock(spec=RedisClient),
            dlocks={"dl": RedisDistributedLockConfig(namespace="x")},
        )()
        ctx2 = context_from_deps(deps)
        q = ctx2.dlock.query(spec)
        c = ctx2.dlock.command(spec)
        assert isinstance(q, RedisDistributedLockAdapter)
        assert isinstance(c, RedisDistributedLockAdapter)
