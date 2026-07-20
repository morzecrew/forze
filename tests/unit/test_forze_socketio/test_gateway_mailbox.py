"""Gateway store-then-forward — durable principal signals are mailboxed (M2)."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.realtime import (
    Audience,
    AudienceKind,
    RealtimeEvent,
    RealtimeEventCatalog,
    RealtimeSignal,
)
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import realtime_inbox_spec
from forze_mock import MockDepsModule
from forze_socketio import (
    GatewayDedup,
    InMemoryRealtimeMailbox,
    InMemoryRealtimePresence,
    RealtimeGateway,
    RealtimeGatewayStats,
    RealtimeMailbox,
    RealtimeSignalSource,
    SignalHandler,
)

# ----------------------- #

_TENANT = UUID("11111111-1111-1111-1111-111111111111")
_HLC = HlcTimestamp(physical_ms=1, logical=0)


class _MsgView(BaseModel):
    text: str


class _StubSio:
    def __init__(self) -> None:
        self.emits: list[dict[str, Any]] = []
        self.fail = False  # when set, every emit raises (a failed live delivery)

    async def emit(self, event: str, data: Any = None, *, namespace: str | None = None,
                   room: str | None = None, **_: Any) -> None:
        if self.fail:
            raise RuntimeError("boom")
        self.emits.append({"event": event, "data": data, "room": room})


class _NullSource(RealtimeSignalSource):
    async def run(
        self,
        ctx: ExecutionContext,
        handler: SignalHandler,
        *,
        stop: asyncio.Event | None = None,
    ) -> None:  # pragma: no cover
        raise NotImplementedError


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _gateway(sio: _StubSio, **kw: Any) -> RealtimeGateway:
    return RealtimeGateway(
        sio=sio,  # type: ignore[arg-type]
        source=_NullSource(),
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
        **kw,
    )


def _principal_signal(text: str = "hi") -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


async def _drive(gw: RealtimeGateway, mailbox: RealtimeMailbox | None, signal: RealtimeSignal,
                 dedup_id: str | None = "evt-1") -> None:
    """Run one signal through the gateway's durable handler with *mailbox* injected."""

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, mailbox, signal, _TENANT, dedup_id, _HLC)


# ----------------------- #


async def test_durable_principal_signal_is_stored_and_emitted() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    await _drive(_gateway(sio), mailbox, _principal_signal())

    assert [r.event_id for r in await mailbox.read_since(principal="u1", since=None)] == ["evt-1"]
    assert sio.emits[0]["data"] == {"id": "evt-1", "data": {"text": "hi"}}  # emitted live


async def test_redelivered_durable_signal_is_stored_once() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    gw = _gateway(sio)
    runtime = _runtime()  # share one runtime so the inbox dedup persists across deliveries
    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, mailbox, _principal_signal(), _TENANT, "evt-1", _HLC)
        await gw._handle(ctx, mailbox, _principal_signal(), _TENANT, "evt-1", _HLC)  # relay retry

    assert len(await mailbox.read_since(principal="u1", since=None)) == 1  # inbox dedup
    assert len(sio.emits) == 1


async def test_mailboxed_signal_survives_and_acks_when_live_emit_fails() -> None:
    # recoverable path: the store commits with the dedup mark, THEN the live emit runs. The
    # emit is best-effort — a failure is swallowed (not raised), so _handle returns normally
    # and the caller can ack the durable message instead of leaving it pending forever. The
    # recipient still gets it via reconnect-replay (the mailbox is the delivery guarantee).
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    sio.fail = True
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _gateway(sio)._handle(ctx, mailbox, _principal_signal(), _TENANT, "evt-1", _HLC)

    assert [r.event_id for r in await mailbox.read_since(principal="u1", since=None)] == ["evt-1"]
    assert sio.emits == []  # the live emit failed, but the signal survives in the mailbox


async def test_topic_signal_is_not_mailboxed() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    signal = RealtimeSignal.of(Audience.topic("room"), "message.new", {"text": "x"})
    await _drive(_gateway(sio), mailbox, signal)

    assert await mailbox.read_since(principal="room", since=None) == []  # no per-recipient mailbox
    assert len(sio.emits) == 1  # but still emitted live


async def test_ephemeral_signal_is_not_mailboxed() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    await _drive(_gateway(sio), mailbox, _principal_signal(), dedup_id=None)  # ephemeral

    assert await mailbox.read_since(principal="u1", since=None) == []
    assert sio.emits[0]["data"] == {"id": None, "data": {"text": "hi"}}


async def test_offline_delivery_opt_out_is_not_mailboxed() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    catalog = RealtimeEventCatalog.of(
        RealtimeEvent(name="order.shipped", payload_type=_MsgView, offline_delivery=False)
    )
    await _drive(_gateway(sio, event_catalog=catalog), mailbox, _principal_signal())

    assert await mailbox.read_since(principal="u1", since=None) == []  # opted out
    assert len(sio.emits) == 1  # still emitted live


# ----------------------- #
# a declared catalog closes the emitted surface (undeclared / off-shape signals are dropped)


def _catalog() -> RealtimeEventCatalog:
    return RealtimeEventCatalog.of(RealtimeEvent(name="order.shipped", payload_type=_MsgView))


async def test_catalogued_signal_passes_and_is_emitted() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    await _drive(_gateway(sio, event_catalog=_catalog()), mailbox, _principal_signal())

    assert len(sio.emits) == 1  # declared event + valid payload → admitted


async def test_undeclared_event_is_rejected_and_not_emitted() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    rogue = RealtimeSignal.of(Audience.principal("u1"), "totally.undeclared", {"text": "x"})
    await _drive(_gateway(sio, event_catalog=_catalog()), mailbox, rogue)

    assert sio.emits == []  # never reaches a client
    assert await mailbox.read_since(principal="u1", since=None) == []  # nor the mailbox


async def test_malformed_payload_is_rejected_and_not_emitted() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    # declared event, but the payload doesn't match _MsgView (missing "text")
    bad = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"wrong": 1})
    await _drive(_gateway(sio, event_catalog=_catalog()), mailbox, bad)

    assert sio.emits == []
    assert await mailbox.read_since(principal="u1", since=None) == []


class _CountView(BaseModel):
    count: int


async def test_emitted_and_stored_payload_is_catalog_normalized() -> None:
    # a catalog normalizes the payload to the declared model's JSON shape before emit: a raw
    # string "1" for an int field reaches the client (and the mailbox) coerced to 1, so the
    # emitted contract matches the declared event — not the raw producer payload.
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    catalog = RealtimeEventCatalog.of(RealtimeEvent(name="counter", payload_type=_CountView))
    raw = RealtimeSignal.of(Audience.principal("u1"), "counter", {"count": "1"})
    await _drive(_gateway(sio, event_catalog=catalog), mailbox, raw)

    assert sio.emits[0]["data"] == {"id": "evt-1", "data": {"count": 1}}  # int, not "1"
    stored = await mailbox.read_since(principal="u1", since=None)
    assert stored[0].payload == {"count": 1}  # replay matches the normalized live frame


async def test_disallowed_audience_kind_is_rejected() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    catalog = RealtimeEventCatalog.of(
        RealtimeEvent(
            name="message.new",
            payload_type=_MsgView,
            audience_kinds=frozenset({AudienceKind.TOPIC}),
        )
    )
    # message.new is topic-only; a principal-addressed signal is off-contract
    off = RealtimeSignal.of(Audience.principal("u1"), "message.new", {"text": "x"})
    await _drive(_gateway(sio, event_catalog=catalog), mailbox, off)

    assert sio.emits == []


class _TenantRequiredMailbox(RealtimeMailbox):
    """A tenant-aware mailbox standing in for the adapter's fail-closed behaviour:
    with no tenant bound, ``store`` raises the adapter's bare ``tenant_required``."""

    async def store(self, **_: Any) -> None:
        raise exc.authentication("Tenant ID is required", code="tenant_required")

    async def read_since(self, **_: Any) -> list[Any]:  # pragma: no cover
        return []

    async def position_of(self, **_: Any) -> None:  # pragma: no cover
        return None

    async def trim(self, **_: Any) -> None:  # pragma: no cover
        return None


async def test_tenant_aware_mailbox_without_binding_fails_with_actionable_error() -> None:
    # The gateway has no ambient tenant and bind_tenant_from_headers is off (default),
    # so a tenant-aware mailbox cannot scope — the opaque tenant_required is rewrapped
    # into an error naming the wiring contract. Deliberately NOT a configuration kind:
    # in the consume loop a configuration verdict is process-terminal (supervision
    # stops for every tenant, the message redelivers on restart, and the loop dies
    # again). A per-signal kind parks the one message for the poison ceiling instead.
    sio = _StubSio()

    with pytest.raises(CoreException) as caught:
        await _drive(_gateway(sio), _TenantRequiredMailbox(), _principal_signal())

    assert caught.value.code == "realtime_mailbox_tenant_unbound"
    assert caught.value.kind is not ExceptionKind.CONFIGURATION
    assert "bind_tenant_from_headers" in caught.value.summary


async def test_presence_skips_live_emit_when_offline_but_still_stores() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    presence = InMemoryRealtimePresence()  # nobody joined → count 0
    await _drive(_gateway(sio, presence=presence), mailbox, _principal_signal())

    assert [r.event_id for r in await mailbox.read_since(principal="u1", since=None)] == ["evt-1"]
    assert sio.emits == []  # offline → live emit skipped


# ----------------------- #
# require_tenant: an untenanted signal is refused, never emitted to the global room


async def test_require_tenant_drops_untenanted_signal() -> None:
    # the room name is the isolation boundary — with require_tenant the gateway drops a
    # signal that resolves no tenant (missing/malformed header) instead of degrading to
    # the unprefixed global room, and the drop is counted
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    stats = RealtimeGatewayStats()
    gw = _gateway(sio, require_tenant=True, stats=stats)

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await gw._handle(ctx, mailbox, _principal_signal(), None, "evt-1", _HLC)  # no tenant

    assert sio.emits == []  # nothing reaches the global room
    assert await mailbox.read_since(principal="u1", since=None) == []  # nor the mailbox
    assert stats.untenanted_dropped == 1


async def test_require_tenant_passes_tenanted_signal_to_the_scoped_room() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    gw = _gateway(sio, require_tenant=True)
    await _drive(gw, mailbox, _principal_signal())  # _drive binds _TENANT

    assert sio.emits[0]["room"] == f"t:{_TENANT}:principal:u1"  # tenant-prefixed, never global
