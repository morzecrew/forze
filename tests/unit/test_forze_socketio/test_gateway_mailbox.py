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
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.primitives import HlcTimestamp
from forze_kits.integrations.realtime import realtime_inbox_spec
from forze_socketio import (
    GatewayDedup,
    InMemoryRealtimeMailbox,
    InMemoryRealtimePresence,
    RealtimeGateway,
    RealtimeMailbox,
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


async def test_presence_skips_live_emit_when_offline_but_still_stores() -> None:
    sio, mailbox = _StubSio(), InMemoryRealtimeMailbox()
    presence = InMemoryRealtimePresence()  # nobody joined → count 0
    await _drive(_gateway(sio, presence=presence), mailbox, _principal_signal())

    assert [r.event_id for r in await mailbox.read_since(principal="u1", since=None)] == ["evt-1"]
    assert sio.emits == []  # offline → live emit skipped
