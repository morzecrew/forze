"""Gateway store-then-forward — durable principal signals are mailboxed (M2)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.realtime import (
    Audience,
    RealtimeEvent,
    RealtimeEventCatalog,
    RealtimeSignal,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import realtime_inbox_spec
from forze_socketio import (
    GatewayDedup,
    InMemoryRealtimeMailbox,
    InMemoryRealtimePresence,
    RealtimeGateway,
    RealtimeSignalSource,
    SignalHandler,
)
from forze_mock import MockDepsModule

# ----------------------- #

_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_HLC = HlcTimestamp(physical_ms=1, logical=0)


class _MsgView(BaseModel):
    text: str


class _StubSio:
    def __init__(self) -> None:
        self.emits: list[dict[str, Any]] = []

    async def emit(self, event: str, data: Any = None, *, namespace: str | None = None,
                   room: str | None = None, **_: Any) -> None:
        self.emits.append({"event": event, "data": data, "room": room})


class _NullSource(RealtimeSignalSource):
    async def run(self, ctx: ExecutionContext, handler: SignalHandler) -> None:  # pragma: no cover
        raise NotImplementedError


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _gateway(sio: _StubSio, mailbox: InMemoryRealtimeMailbox, **kw: Any) -> RealtimeGateway:
    return RealtimeGateway(
        sio=sio,  # type: ignore[arg-type]
        source=_NullSource(),
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
        mailbox=mailbox,
        **kw,
    )


def _principal_signal(text: str = "hi") -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


# ----------------------- #


async def test_durable_principal_signal_is_stored_and_emitted() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    gw = _gateway(sio, mailbox)

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, _principal_signal(), _TENANT, "evt-1", _HLC)
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
            rows = await mailbox.read_since(ctx, principal="u1", since=None)

    assert [r.event_id for r in rows] == ["evt-1"]  # stored
    assert sio.emits[0]["data"] == {"id": "evt-1", "data": {"text": "hi"}}  # emitted live


async def test_redelivered_durable_signal_is_stored_once() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    gw = _gateway(sio, mailbox)

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, _principal_signal(), _TENANT, "evt-1", _HLC)
        await gw._handle(ctx, _principal_signal(), _TENANT, "evt-1", _HLC)  # relay retry / claim
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
            rows = await mailbox.read_since(ctx, principal="u1", since=None)

    assert len(rows) == 1  # inbox dedup → stored + emitted once
    assert len(sio.emits) == 1


async def test_topic_signal_is_not_mailboxed() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    gw = _gateway(sio, mailbox)
    signal = RealtimeSignal.of(Audience.topic("room"), "message.new", {"text": "x"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, signal, _TENANT, "evt-1", _HLC)
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
            rows = await mailbox.read_since(ctx, principal="room", since=None)

    assert rows == []  # topic has no per-recipient mailbox
    assert len(sio.emits) == 1  # but still emitted live


async def test_ephemeral_signal_is_not_mailboxed() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    gw = _gateway(sio, mailbox)

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, _principal_signal(), _TENANT, None, _HLC)  # no dedup id = ephemeral
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
            rows = await mailbox.read_since(ctx, principal="u1", since=None)

    assert rows == []
    assert sio.emits[0]["data"] == {"id": None, "data": {"text": "hi"}}


async def test_offline_delivery_opt_out_is_not_mailboxed() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    catalog = RealtimeEventCatalog.of(
        RealtimeEvent(name="order.shipped", payload_type=_MsgView, offline_delivery=False)
    )
    gw = _gateway(sio, mailbox, event_catalog=catalog)

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, _principal_signal(), _TENANT, "evt-1", _HLC)
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
            rows = await mailbox.read_since(ctx, principal="u1", since=None)

    assert rows == []  # opted out of offline delivery
    assert len(sio.emits) == 1  # still emitted live


async def test_presence_skips_live_emit_when_offline_but_still_stores() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    presence = InMemoryRealtimePresence()  # nobody joined → count 0
    gw = _gateway(sio, mailbox, presence=presence)

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, _principal_signal(), _TENANT, "evt-1", _HLC)
        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=_TENANT)):
            rows = await mailbox.read_since(ctx, principal="u1", since=None)

    assert [r.event_id for r in rows] == ["evt-1"]  # stored for reconnect
    assert sio.emits == []  # offline → live emit skipped
