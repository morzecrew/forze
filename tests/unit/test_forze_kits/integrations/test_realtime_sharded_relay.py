"""Namespace-tier outbox — a bound relay pass drains each tenant's partition (RFC 0007, E).

The durable on-ramp's missing leg: when the realtime **outbox** is itself tenant-aware
(partitioned), the tenant-less relay can't read it — it must drain each tenant's partition
under a bound tenant. This proves a per-tenant bound relay pass (what the sharded relay's
``_drain_tick`` runs) reads a partitioned outbox and routes each signal to that tenant's
stream key, with full isolation. Both the outbox and the stream are tenant-aware here.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, RealtimeEvent
from forze.application.contracts.stream import StreamQueryDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.realtime import (
    build_realtime_publisher,
    realtime_outbox_spec,
    realtime_stream_spec,
)
from forze_mock import MockDepsModule, MockRouteConfig

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")

_STREAM = realtime_stream_spec()
_OUTBOX = realtime_outbox_spec(name="realtime-outbox", stream=str(_STREAM.name))


class _Shipped(BaseModel):
    order: str


_SHIPPED = RealtimeEvent(name="order.shipped", payload_type=_Shipped)


def _runtime() -> ExecutionRuntime:
    # BOTH the outbox and the stream are tenant-aware (partitioned) — namespace-tier outbox
    routes = {
        str(_OUTBOX.name): MockRouteConfig(tenant_aware=True),
        str(_STREAM.name): MockRouteConfig(tenant_aware=True),
    }
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze())


async def _stage(rt: ExecutionRuntime, tenant: UUID, order: str) -> None:
    # a handler stages under its tenant in its own unit-of-work; the row lands in that
    # tenant's outbox partition (separate scopes so each flush is under its own tenant)
    async with rt.scope():
        ctx = rt.get_context()
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            pub = build_realtime_publisher(ctx, stream_spec=_STREAM, outbox_spec=_OUTBOX)
            await pub.stage(Audience.principal("alice"), _SHIPPED, _Shipped(order=order))
            await ctx.outbox.command(_OUTBOX).flush()


async def _orders_on_stream(ctx, tenant: UUID) -> list[str]:  # type: ignore[no-untyped-def]
    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        stream = ctx.deps.resolve_configurable(ctx, StreamQueryDepKey, _STREAM, route=_STREAM.name)
        messages = await stream.read({str(_STREAM.name): "0"})
    return [m.payload.payload["order"] for m in messages]


async def test_sharded_relay_drains_each_tenant_partition_to_its_stream() -> None:
    rt = _runtime()
    await _stage(rt, _T1, "acme-1001")
    await _stage(rt, _T2, "globex-2002")

    async with rt.scope():
        ctx = rt.get_context()
        # the sharded relay: one bound pass per assigned tenant (what _drain_tick runs)
        for tenant in (_T1, _T2):
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                await OutboxRelay(outbox_spec=_OUTBOX).to_stream(ctx, _STREAM)

        # each durable signal landed on its own tenant's stream key — partitioned end to end
        assert await _orders_on_stream(ctx, _T1) == ["acme-1001"]
        assert await _orders_on_stream(ctx, _T2) == ["globex-2002"]
