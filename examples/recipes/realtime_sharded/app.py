"""Recipe: tenant-aware (namespace-tier) realtime — per-tenant streams, trusted isolation.

The default realtime stream is tenant-global: one stream carries every tenant's signals
and the tenant rides an (untrusted) header. For **trusted** per-tenant isolation you put the
stream on the tenancy tier ladder: wire the realtime **stream** route
``tenant_aware`` so each tenant gets its own key/partition, and consume with
``TenantShardedSignalSource`` — one consume loop per assigned tenant, each bound to that
tenant. The tenant a signal belongs to is then the stream it was read from (set by the
publisher's ambient tenant at write time), never a forgeable header.

This recipe shows the part that makes that work end to end for **durable** signals: a signal
staged under tenant A travels through the outbox and the relay, and the relay — a tenant-less
background drain — forwards each row under the tenant it was staged with, so the append lands
on A's stream key. A per-tenant consumer (what ``TenantShardedSignalSource`` automates, here
shown as ``read_tenant_stream``) sees only its own tenant's signals. The **outbox stays
tenant-global** (tagged: rows carry their ``tenant_id``); only the **stream** is tenant-aware.
Mock-runnable, no sockets.

Run it:  uv run python -m examples.recipes.realtime_sharded.app
Exercised by tests/unit/test_examples/test_realtime_sharded.py.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, RealtimeEvent, RealtimeSignal
from forze.application.contracts.stream import StreamQueryDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.realtime import (
    build_realtime_publisher,
    realtime_outbox_spec,
    realtime_stream_spec,
)
from forze_mock import MockDepsModule
from forze_mock.execution import MockRouteConfig

# --8<-- [start:setup]
ACME = UUID("11111111-1111-1111-1111-111111111111")  # tenant A
GLOBEX = UUID("22222222-2222-2222-2222-222222222222")  # tenant B

STREAM = realtime_stream_spec()  # the per-tenant stream (wired tenant_aware below)
# The outbox keeps its own name so it can stay tenant-global while the stream is tenant-aware
# (the mock keys routes by spec name; in production they are separate backends).
OUTBOX = realtime_outbox_spec(name="realtime-outbox", stream=str(STREAM.name))


class _OrderShipped(BaseModel):
    order: str


SHIPPED = RealtimeEvent(name="order.shipped", payload_type=_OrderShipped)


def _context() -> ExecutionContext:
    routes = {
        str(STREAM.name): MockRouteConfig(tenant_aware=True),  # per-tenant stream key
        # "realtime-outbox" is intentionally absent → tenant-global (tagged) outbox
    }
    return ExecutionContext(
        deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze().resolve()
    )


# --8<-- [end:setup]


# --8<-- [start:publish]
async def publish_durable(ctx: ExecutionContext, *, tenant: UUID, order: str) -> None:
    """A handler stages a durable signal under its tenant — no realtime/tenant plumbing.

    The signal is addressed to a principal; the staging tenant is ambient. The outbox row
    is tagged with that tenant, which the relay later uses to route it. (A real handler
    flushes in its own transaction; here both tenants stage into one buffer, flushed once
    by :func:`relay` so the in-process demo shares a single mock store.)
    """

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        publisher = build_realtime_publisher(
            ctx, stream_spec=STREAM, outbox_spec=OUTBOX
        )
        await publisher.stage(
            Audience.principal("alice"), SHIPPED, _OrderShipped(order=order)
        )


# --8<-- [end:publish]


# --8<-- [start:relay]
async def relay(ctx: ExecutionContext) -> None:
    """The background relay drains the (tenant-global) outbox and forwards each row.

    It runs with **no** tenant bound, yet routes correctly: it binds each row's staged
    tenant before appending, so the durable signal lands on that tenant's stream key.
    """

    await ctx.outbox.command(
        OUTBOX
    ).flush()  # write staged rows to the tenant-global outbox
    await OutboxRelay(outbox_spec=OUTBOX).to_stream(ctx, STREAM)


# --8<-- [end:relay]


# --8<-- [start:consume]
async def read_tenant_stream(
    ctx: ExecutionContext, tenant: UUID
) -> list[RealtimeSignal]:
    """What ``TenantShardedSignalSource`` does per assigned tenant: bind it, read its stream.

    Binding the tenant resolves the stream adapter to *that tenant's* key, so this only ever
    sees the bound tenant's signals — the isolation is the stream's, not a header check.
    """

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        stream = ctx.deps.resolve_configurable(
            ctx, StreamQueryDepKey, STREAM, route=STREAM.name
        )
        messages = await stream.read({str(STREAM.name): "0"})

    return [m.payload for m in messages]


# --8<-- [end:consume]


async def main() -> None:
    ctx = _context()

    # Two tenants ship an order to a same-named principal "alice" — different people.
    await publish_durable(ctx, tenant=ACME, order="acme-1001")
    await publish_durable(ctx, tenant=GLOBEX, order="globex-2002")

    # One tenant-less relay pass routes each durable signal to its tenant's stream key.
    await relay(ctx)

    acme = await read_tenant_stream(ctx, ACME)
    globex = await read_tenant_stream(ctx, GLOBEX)

    print(f"acme stream:   {[s.payload['order'] for s in acme]}")
    print(f"globex stream: {[s.payload['order'] for s in globex]}")


if __name__ == "__main__":
    asyncio.run(main())
