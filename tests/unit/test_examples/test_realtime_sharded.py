"""Namespace-tier realtime recipe — durable signals routed per-tenant by the relay (mock)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

from examples.recipes.realtime_sharded.app import (
    ACME,
    GLOBEX,
    _context,
    assign_shard,
    publish_durable,
    read_tenant_stream,
    relay,
)
from forze.application.contracts.fleet import FleetSpec


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


async def test_replicas_started_together_split_the_tenants() -> None:
    ctx = _context()
    spec = FleetSpec(name="gateways", ttl=timedelta(milliseconds=300))
    tenants = [uuid4() for _ in range(40)]
    # A port whose own spec is shorter: the recipe's entries must live as long as it waits.
    fleet = ctx.fleet(FleetSpec(name="gateways", ttl=timedelta(milliseconds=60)))

    a, b = await asyncio.gather(
        assign_shard(fleet, "gw-a", tenants, spec=spec),
        assign_shard(fleet, "gw-b", tenants, spec=spec),
    )

    # each tenant has exactly one owner, and both replicas got some
    assert sorted(a.tenants + b.tenants) == sorted(tenants)
    assert not set(a.tenants) & set(b.tenants)
    assert a.tenants and b.tenants


async def test_a_lone_replica_takes_every_tenant() -> None:
    spec = FleetSpec(name="gateways", ttl=timedelta(milliseconds=20))
    tenants = [uuid4() for _ in range(5)]

    shard = await assign_shard(_context().fleet(spec), "gw-only", tenants, spec=spec)

    assert shard.tenants == tuple(tenants)
