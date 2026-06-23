"""Tenant-sharded gateway source — namespace-tier per-tenant realtime streams (RFC 0007).

The gateway is a cross-tenant consumer with no ambient tenant. :class:`TenantShardedSignalSource`
runs one consume loop per assigned tenant, each **bound** to that tenant, so the tenant a signal
belongs to is the stream it was read from (set by the producer's ambient tenant at write time) —
**not** a per-message header. These tests prove the isolation is the stream's (signals carry no
``forze_tenant_id`` header) and that a tenant-aware mailbox therefore scopes by a *trusted* tenant
with ``bind_tenant_from_headers`` left off (no fail-closed) — the gap the interim fix only named.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.realtime import Audience, RealtimeEvent, RealtimeSignal
from forze.application.contracts.stream import StreamCommandDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.realtime import (
    build_realtime_mailbox,
    build_realtime_publisher,
    realtime_cursor_spec,
    realtime_inbox_spec,
    realtime_mailbox_spec,
    realtime_outbox_spec,
    realtime_stream_spec,
)
from forze_socketio import (
    GatewayDedup,
    RealtimeGateway,
    RealtimeShard,
    TenantShardedSignalSource,
)
from forze_mock import MockDepsModule, MockRouteConfig

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")
_FAST = timedelta(seconds=0.01)


class _MsgView(BaseModel):
    text: str


_ORDER_SHIPPED = RealtimeEvent(name="order.shipped", payload_type=_MsgView)


class _StubSio:
    def __init__(self) -> None:
        self.emits: list[dict[str, Any]] = []

    async def emit(self, event: str, data: Any = None, *, namespace: str | None = None,
                   room: str | None = None, **_: Any) -> None:
        self.emits.append({"event": event, "data": data, "room": room})


def _tenant_aware(*spec_names: str) -> MockDepsModule:
    return MockDepsModule(
        routes={name: MockRouteConfig(tenant_aware=True) for name in spec_names}
    )


def _runtime(module: MockDepsModule) -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


def _shard(spec: Any, tenants: list[UUID]) -> RealtimeShard:
    return RealtimeShard(stream_spec=spec, tenants=lambda: tenants)


def _sharded_gateway(sio: _StubSio, spec: Any, *, tenants: list[UUID], **kw: Any) -> RealtimeGateway:
    return RealtimeGateway(
        sio=sio,  # type: ignore[arg-type]
        source=TenantShardedSignalSource(shard=_shard(spec, tenants), poll_interval=_FAST),
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
        **kw,
    )


async def _append_for(ctx: Any, spec: Any, tenant: UUID, signal: RealtimeSignal,
                      *, event_id: str | None = None) -> None:
    """Append into *tenant*'s stream partition with **no** tenant header — isolation is
    the stream's, so the gateway must take the tenant from the stream it read from."""

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        cmd = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
        headers = {HEADER_EVENT_ID: event_id} if event_id else {}
        await cmd.append(str(spec.name), signal, type=signal.event, headers=headers)


async def _run_settle(gw: RealtimeGateway, ctx: Any, predicate: Any,
                      *, settle: float = 0.06, timeout: float = 2.0) -> None:
    task = asyncio.create_task(gw.run(ctx))
    try:
        waited = 0.0
        while not predicate() and waited < timeout:
            await asyncio.sleep(0.01)
            waited += 0.01
        await asyncio.sleep(settle)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# ----------------------- #


async def test_sharded_source_scopes_each_tenant_from_stream_not_header() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _sharded_gateway(sio, spec, tenants=[_T1, _T2])

    runtime = _runtime(_tenant_aware(str(spec.name)))
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append_for(ctx, spec, _T1, RealtimeSignal.of(Audience.principal("u1"), "e", {"v": "a"}))
        await _append_for(ctx, spec, _T2, RealtimeSignal.of(Audience.principal("u2"), "e", {"v": "b"}))
        await _run_settle(gw, ctx, lambda: len(sio.emits) >= 2)

    # each signal emits to its own tenant-scoped room — derived from the stream, not a header
    assert {e["room"] for e in sio.emits} == {f"t:{_T1}:principal:u1", f"t:{_T2}:principal:u2"}


async def test_sharded_source_isolates_same_principal_across_tenants() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _sharded_gateway(sio, spec, tenants=[_T1, _T2])
    sig = RealtimeSignal.of(Audience.principal("u1"), "e", {"v": "x"})

    runtime = _runtime(_tenant_aware(str(spec.name)))
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append_for(ctx, spec, _T1, sig)
        await _append_for(ctx, spec, _T2, sig)  # same principal, different tenant
        await _run_settle(gw, ctx, lambda: len(sio.emits) >= 2)

    # the same principal in two tenants never shares a room — no cross-tenant leak
    assert sorted(e["room"] for e in sio.emits) == sorted(
        [f"t:{_T1}:principal:u1", f"t:{_T2}:principal:u1"]
    )


async def test_unassigned_tenant_is_not_consumed() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _sharded_gateway(sio, spec, tenants=[_T1])  # T2 not in this shard

    runtime = _runtime(_tenant_aware(str(spec.name)))
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append_for(ctx, spec, _T1, RealtimeSignal.of(Audience.principal("u1"), "e", {"v": "a"}))
        await _append_for(ctx, spec, _T2, RealtimeSignal.of(Audience.principal("u9"), "e", {"v": "z"}))
        await _run_settle(gw, ctx, lambda: bool(sio.emits))

    assert [e["room"] for e in sio.emits] == [f"t:{_T1}:principal:u1"]  # only the assigned tenant


async def test_durable_signal_relayed_to_per_tenant_stream_reaches_sharded_gateway() -> None:
    # The FULL durable namespace path through the real relay: stage under tenant A →
    # outbox → OutboxRelay (run unbound, as the background relay does) → per-tenant stream
    # key → sharded gateway delivers and mailboxes under the trusted tenant. The relay must
    # bind the claim's tenant so the append lands on T1's key, not the global one.
    stream = realtime_stream_spec()
    # The outbox stays tenant-global, under a name distinct from the stream so the mock's
    # name-keyed routes can scope them independently (in production they are separate
    # backends with independent configs even under one channel name).
    outbox = realtime_outbox_spec(name="realtime-outbox", stream=str(stream.name))
    sio = _StubSio()
    gw = RealtimeGateway(
        sio=sio,  # type: ignore[arg-type]
        source=TenantShardedSignalSource(shard=_shard(stream, [_T1, _T2]), poll_interval=_FAST),
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
        mailbox_factory=build_realtime_mailbox,
    )
    # The outbox stays tenant-global (tagged rows); only the stream + mailbox are tenant-aware.
    module = MockDepsModule(routes={
        str(stream.name): MockRouteConfig(tenant_aware=True),  # stream is per-tenant
        str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True),
        str(realtime_cursor_spec().name): MockRouteConfig(tenant_aware=True),
        # "realtime-outbox" is intentionally absent → tenant-global outbox
    })
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_T1)):
            pub = build_realtime_publisher(ctx, stream_spec=stream, outbox_spec=outbox)
            await pub.stage(Audience.principal("u1"), _ORDER_SHIPPED, _MsgView(text="shipped"))
            await ctx.outbox.command(outbox).flush()

        # relay runs UNBOUND, like the background lifecycle step does
        await OutboxRelay(outbox_spec=outbox).to_stream(ctx, stream)
        await _run_settle(gw, ctx, lambda: bool(sio.emits))

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_T1)):
            t1 = await build_realtime_mailbox(ctx).read_since(principal="u1", since=None)
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_T2)):
            t2 = await build_realtime_mailbox(ctx).read_since(principal="u1", since=None)

    assert sio.emits[0]["room"] == f"t:{_T1}:principal:u1"  # delivered to T1's room
    assert len(t1) == 1 and t2 == []  # mailboxed under the trusted tenant, isolated from T2


async def test_empty_shard_idles_until_cancelled() -> None:
    # An unassigned instance must idle, not return (a returning run task looks like a
    # crash to supervision) and not busy-loop.
    spec = realtime_stream_spec()
    gw = _sharded_gateway(_StubSio(), spec, tenants=[])

    runtime = _runtime(_tenant_aware(str(spec.name)))
    async with runtime.scope():
        ctx = runtime.get_context()
        task = asyncio.create_task(gw.run(ctx))
        await asyncio.sleep(0.05)
        assert not task.done()  # idling under an empty shard
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


async def test_tenant_aware_mailbox_scopes_by_trusted_tenant_without_header_binding() -> None:
    # The RFC 0007 payoff: a tenant-aware mailbox + sharded source + bind_tenant_from_headers
    # OFF (default). The store must NOT fail closed — the tenant is the (trusted) stream's.
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = RealtimeGateway(
        sio=sio,  # type: ignore[arg-type]
        source=TenantShardedSignalSource(shard=_shard(spec, [_T1, _T2]), poll_interval=_FAST),
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
        mailbox_factory=build_realtime_mailbox,
    )
    eid = str(UUID(int=1))
    sig = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "hi"})

    module = _tenant_aware(
        str(spec.name), str(realtime_mailbox_spec().name), str(realtime_cursor_spec().name)
    )
    runtime = _runtime(module)
    async with runtime.scope():
        ctx = runtime.get_context()
        # durable (carries an event id) → mailboxed; appended into T1's partition
        await _append_for(ctx, spec, _T1, sig, event_id=eid)
        await _run_settle(gw, ctx, lambda: bool(sio.emits))

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_T1)):
            t1 = await build_realtime_mailbox(ctx).read_since(principal="u1", since=None)
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_T2)):
            t2 = await build_realtime_mailbox(ctx).read_since(principal="u1", since=None)

    assert [e.event_id for e in t1] == [eid]  # stored under the trusted shard tenant…
    assert t2 == []  # …and isolated from the other tenant
    assert sio.emits[0]["room"] == f"t:{_T1}:principal:u1"
