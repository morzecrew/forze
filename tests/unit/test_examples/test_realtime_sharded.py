"""Namespace-tier realtime recipe — durable signals routed per-tenant by the relay (mock)."""

from __future__ import annotations

from examples.recipes.realtime_sharded.app import (
    ACME,
    GLOBEX,
    _context,
    publish_durable,
    read_tenant_stream,
    relay,
)


async def test_durable_signals_route_to_their_own_tenant_stream() -> None:
    ctx = _context()

    # two tenants ship to a same-named principal; the outbox stays tenant-global (tagged)
    await publish_durable(ctx, tenant=ACME, order="acme-1001")
    await publish_durable(ctx, tenant=GLOBEX, order="globex-2002")

    # one tenant-less relay pass routes each row to its own tenant's stream key
    await relay(ctx)

    acme = await read_tenant_stream(ctx, ACME)
    globex = await read_tenant_stream(ctx, GLOBEX)

    # each tenant's stream holds only its own signal — trusted isolation, no header check
    assert [s.payload["order"] for s in acme] == ["acme-1001"]
    assert [s.payload["order"] for s in globex] == ["globex-2002"]
