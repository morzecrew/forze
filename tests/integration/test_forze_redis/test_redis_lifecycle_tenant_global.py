"""The singleton guard and leadership against a real Redis, with no tenant bound at startup.

A lock route that is tenant-aware, or whose client is routed per tenant, refuses every call
made outside a tenant. Both steps now refuse it at startup as wiring; a tenant-global route
still runs.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from testcontainers.redis import RedisContainer

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.secrets import SecretRef
from forze.application.execution import ExecutionContext, build_runtime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.integrations.realtime import (
    realtime_group_ensure_lifecycle_step,
    realtime_stream_spec,
)
from forze_kits.lifecycle import (
    LeadershipSpec,
    Term,
    leadership_lifecycle_step,
    singleton_lifecycle_step,
)
from forze_redis import (
    RedisCacheConfig,
    RedisDepsModule,
    RedisDistributedLockConfig,
    RedisStreamGroupConfig,
)
from forze_redis.kernel.client import RedisClient, RedisClientPort, RoutedRedisClient

pytestmark = pytest.mark.asyncio

# ----------------------- #

LOCK = DistributedLockSpec(name="lifecycle", ttl=timedelta(seconds=10))


class _Secrets:
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn

    async def resolve_str(self, ref: SecretRef) -> str:
        return self.dsn

    async def exists(self, ref: SecretRef) -> bool:
        return True


@pytest_asyncio.fixture
async def routed_client(redis_container: RedisContainer) -> AsyncIterator[RoutedRedisClient]:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = RoutedRedisClient(
        secrets=_Secrets(f"redis://{host}:{port}/0"),  # type: ignore[arg-type]
        secret_ref_for_tenant=lambda tenant: SecretRef(path=f"tenants/{tenant}"),
        tenant_provider=lambda: None,
    )
    await client.startup()

    yield client

    await client.close()


def _singleton(ran: list[str]) -> LifecycleStep:
    async def startup(ctx: ExecutionContext) -> None:
        ran.append("startup")

    return singleton_lifecycle_step(
        LifecycleStep(id="ensure", startup=startup), spec=LOCK, owner=f"replica-{uuid4()}"
    )


def _leadership(led: asyncio.Event) -> LifecycleStep:
    async def lead(_ctx: ExecutionContext, _term: Term) -> None:
        led.set()
        await asyncio.Event().wait()

    return leadership_lifecycle_step(
        LeadershipSpec(
            lock=LOCK,
            key=f"relay-{uuid4()}",
            renew_every=timedelta(seconds=1),
            campaign_every=timedelta(milliseconds=50),
            yield_grace=timedelta(milliseconds=500),
            release_timeout=timedelta(milliseconds=500),
            margin=timedelta(milliseconds=500),
        ),
        owner=f"replica-{uuid4()}",
        lead=lead,
    )


def _module(client: RedisClientPort, *, tenant_aware: bool, **kwargs: Any) -> RedisDepsModule:
    return RedisDepsModule(
        client=client,
        dlocks={
            str(LOCK.name): RedisDistributedLockConfig(
                namespace=f"it-{uuid4()}", tenant_aware=tenant_aware, **kwargs
            )
        },
    )


async def _refused_at_startup(module: RedisDepsModule, step: LifecycleStep) -> None:
    runtime = build_runtime(module, lifecycle_steps=step, drain_timeout=timedelta(0))

    with pytest.raises(CoreException) as caught:
        async with runtime.scope():
            pass

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert caught.value.code == "tenant_global_required"


# ....................... #


async def test_a_tenant_aware_lock_route_is_refused(redis_client: RedisClient) -> None:
    ran: list[str] = []
    led = asyncio.Event()

    await _refused_at_startup(_module(redis_client, tenant_aware=True), _singleton(ran))
    await _refused_at_startup(_module(redis_client, tenant_aware=True), _leadership(led))

    assert ran == []
    assert not led.is_set()


async def test_a_lock_route_on_a_routed_client_is_refused(
    routed_client: RoutedRedisClient,
) -> None:
    ran: list[str] = []
    led = asyncio.Event()

    await _refused_at_startup(_module(routed_client, tenant_aware=False), _singleton(ran))
    await _refused_at_startup(_module(routed_client, tenant_aware=False), _leadership(led))

    assert ran == []
    assert not led.is_set()


async def test_a_tenant_global_lock_route_runs_both(redis_client: RedisClient) -> None:
    ran: list[str] = []
    led = asyncio.Event()
    runtime = build_runtime(
        _module(redis_client, tenant_aware=False),
        lifecycle_steps=[_singleton(ran), _leadership(led)],
        drain_timeout=timedelta(0),
    )

    async with runtime.scope():
        async with asyncio.timeout(5):
            await led.wait()

    assert ran == ["startup"]


async def test_group_ensure_refuses_a_tenant_aware_stream_group(redis_client: RedisClient) -> None:
    spec = realtime_stream_spec()
    module = RedisDepsModule(
        client=redis_client,
        stream_groups={str(spec.name): RedisStreamGroupConfig(tenant_aware=True)},
    )

    await _refused_at_startup(module, realtime_group_ensure_lifecycle_step(stream_spec=spec))


async def test_a_tenant_global_lock_runs_both_under_a_tenancy_floor(
    redis_client: RedisClient,
) -> None:
    # The floor holds the cache to tagged isolation; the lifecycle lock, used where no
    # tenant is bound, is marked the deployment's and left out of it.
    ran: list[str] = []
    led = asyncio.Event()
    module = RedisDepsModule(
        client=redis_client,
        dlocks={
            str(LOCK.name): RedisDistributedLockConfig(
                namespace=f"it-{uuid4()}", tenant_global=True
            )
        },
        caches={"orders": RedisCacheConfig(namespace=f"it-{uuid4()}", tenant_aware=True)},
        required_tenant_isolation="tagged",
    )
    runtime = build_runtime(
        module,
        lifecycle_steps=[_singleton(ran), _leadership(led)],
        drain_timeout=timedelta(0),
    )

    async with runtime.scope():
        async with asyncio.timeout(5):
            await led.wait()

    assert ran == ["startup"]
