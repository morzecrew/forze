"""The tenant-sharded gateway at N-tenant scale, over real Redis — the fleet-shape leg.

The unit and two-tenant integration tests prove the sharded source's semantics; this proves
the *shape* holds when a shard actually looks like a fleet assignment: 24 per-tenant consume
loops on one gateway instance, all supervised, all fed from tenant-prefixed keys on one
logical stream. Three properties: every tenant's signals arrive in that tenant's rooms with
zero cross-tenant leakage; one broken tenant (its consumer group was never provisioned —
the realistic per-tenant wiring fault) degrades only itself while its supervisor keeps
retrying; and a stop brings all N loops down cleanly, between batches, within the grace.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest

pytest.importorskip("redis")

from collections.abc import AsyncIterator

import pytest_asyncio

from forze.application.contracts.deps import Deps
from forze.application.contracts.realtime import (
    Audience,
    RealtimeShard,
    RealtimeSignal,
)
from forze.application.contracts.stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    StreamCommandDepKey,
    StreamSpec,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.serialization import PydanticModelCodec
from forze_kits.integrations.realtime import realtime_tenant_group_ensure_lifecycle_step
from forze_redis.adapters import (
    RedisStreamAdapter,
    RedisStreamCodec,
    RedisStreamGroupAdapter,
    RedisStreamGroupAdminAdapter,
)
from forze_redis.kernel.client import RedisClient, RedisConfig
from forze_socketio import (
    RealtimeGateway,
    TenantShardedSignalSource,
    realtime_gateway_lifecycle_step,
)

# ----------------------- #

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_N = 24
_SIGNALS_PER_TENANT = 2
_TENANTS = tuple(UUID(int=i + 1) for i in range(_N))
_BROKEN = UUID(int=10_000)  # in the shard, but its group is never provisioned
_STREAM = "it-rt-scale"
_GROUP = "scale-gw"


class _RecordingSio:
    def __init__(self) -> None:
        self.rooms: list[str] = []

    async def emit(self, event: str, data: Any = None, *, room: str | None = None, **_: Any) -> None:
        self.rooms.append(room or "")


@pytest_asyncio.fixture(scope="function")
async def scale_client(redis_container) -> AsyncIterator[RedisClient]:  # type: ignore[no-untyped-def]
    """A client sized for N concurrent blocking group reads (the shared fixture's pool of 5
    would serialize 25 loops into a crawl)."""

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)

    client = RedisClient()
    await client.initialize(dsn=f"redis://{host}:{port}/0", config=RedisConfig(max_size=40))

    yield client

    await client.close()


def _runtime(client: RedisClient) -> ExecutionRuntime:
    """Tenant-aware adapters reading the **ambient** tenant — what the sharded source binds."""

    codec = RedisStreamCodec(payload_codec=PydanticModelCodec(RealtimeSignal))

    def module() -> Deps:
        return Deps.plain(
            {
                StreamCommandDepKey: lambda ctx, _spec: RedisStreamAdapter(
                    client=client,
                    codec=codec,
                    tenant_aware=True,
                    tenant_provider=ctx.inv_ctx.get_tenant,
                ),
                AckStreamGroupQueryDepKey: lambda ctx, _spec: RedisStreamGroupAdapter(
                    client=client,
                    codec=codec,
                    tenant_aware=True,
                    tenant_provider=ctx.inv_ctx.get_tenant,
                ),
                AckStreamGroupAdminDepKey: lambda ctx, _spec: RedisStreamGroupAdminAdapter(
                    client=client,
                    tenant_aware=True,
                    tenant_provider=ctx.inv_ctx.get_tenant,
                ),
            }
        )

    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


# ----------------------- #


async def test_sharded_gateway_delivers_n_tenants_and_isolates_the_broken_one(
    scale_client: RedisClient,
) -> None:
    spec: StreamSpec[RealtimeSignal] = StreamSpec(
        name=_STREAM, codec=PydanticModelCodec(model_type=RealtimeSignal)
    )
    healthy_shard = RealtimeShard(stream_spec=spec, tenants=_TENANTS, group=_GROUP)
    # The gateway's shard also carries the broken tenant — provisioned nowhere below.
    gateway_shard = RealtimeShard(
        stream_spec=spec, tenants=(*_TENANTS, _BROKEN), group=_GROUP
    )

    sio = _RecordingSio()
    gateway = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=TenantShardedSignalSource(
            shard=gateway_shard,
            poll_interval=timedelta(milliseconds=100),
            restart_backoff=timedelta(milliseconds=100),
        ),
    )
    step = realtime_gateway_lifecycle_step(gateway, restart_backoff=timedelta(milliseconds=100))
    ensure = realtime_tenant_group_ensure_lifecycle_step(shard=healthy_shard, start_id="0")

    runtime = _runtime(scale_client)

    async with runtime.scope():
        ctx = runtime.get_context()

        # Provision the healthy tenants' groups (the kit step, at scale); publish per tenant
        # under a bound identity — the key prefix, not a header, carries the tenant.
        await ensure.startup(ctx)

        for tenant in _TENANTS:
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                command = ctx.deps.resolve_configurable(
                    ctx, StreamCommandDepKey, spec, route=spec.name
                )
                for k in range(_SIGNALS_PER_TENANT):
                    signal = RealtimeSignal.of(Audience.principal("u1"), "e", {"seq": k})
                    await command.append(_STREAM, signal, type="e")

        await step.startup(ctx)

        expected = _N * _SIGNALS_PER_TENANT
        waited = 0.0
        while len(sio.rooms) < expected and waited < 30.0:
            await asyncio.sleep(0.05)
            waited += 0.05

        # 1. Completeness with zero leakage: every healthy tenant's signals landed in that
        #    tenant's room, exactly _SIGNALS_PER_TENANT each — 24 loops, no cross-wiring.
        assert len(sio.rooms) == expected
        for tenant in _TENANTS:
            assert sio.rooms.count(f"t:{tenant}:principal:u1") == _SIGNALS_PER_TENANT

        # 2. The broken tenant degraded only itself: nothing of its ever emitted (its group
        #    does not exist), and the shard as a whole kept delivering — which the counts
        #    above already prove. Its supervisor is still alive, retrying.
        assert not any(str(_BROKEN) in room for room in sio.rooms)
        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None and not task.done()

        # 3. All N+1 loops stop cleanly within the grace — including the broken tenant's
        #    retry loop — between batches, not via cancellation.
        await step.shutdown(ctx)
        assert task.done() and not task.cancelled()
