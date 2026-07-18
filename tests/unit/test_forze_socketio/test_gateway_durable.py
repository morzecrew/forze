"""Durable on-ramp + gateway exactly-once dedup (E4)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.realtime import (
    Audience,
    RealtimeEvent,
    RealtimeEventCatalog,
    RealtimeSignal,
)
from forze.application.contracts.stream import AckStreamGroupQueryDepKey, StreamCommandDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.realtime import (
    build_realtime_publisher,
    realtime_inbox_spec,
    realtime_outbox_spec,
    realtime_stream_spec,
)
from forze_mock import MockDepsModule
from forze_socketio import GatewayDedup, RealtimeGateway, StreamGroupSignalSource

# ----------------------- #


class _MsgView(BaseModel):
    text: str


_ORDER_SHIPPED = RealtimeEvent(name="order.shipped", payload_type=_MsgView)
_TENANT = TenantIdentity(tenant_id=UUID("11111111-1111-1111-1111-111111111111"))
_FAST = timedelta(seconds=0.01)


class _StubSio:
    def __init__(self) -> None:
        self.emits: list[dict[str, Any]] = []
        self.attempts = 0
        self.fail = False
        self.fail_times = 0  # fail the first N emit attempts, then succeed

    async def emit(self, event: str, data: Any = None, *, namespace: str | None = None,
                   room: str | None = None, **_: Any) -> None:
        self.attempts += 1
        if self.fail or self.attempts <= self.fail_times:
            raise RuntimeError("boom")
        self.emits.append({"event": event, "room": room, "data": data})


def _deduping_gateway(sio: _StubSio, spec, *, reclaim_idle=None) -> RealtimeGateway:  # type: ignore[no-untyped-def]
    source = (
        StreamGroupSignalSource(stream_spec=spec, poll_interval=_FAST, reclaim_idle=reclaim_idle)
        if reclaim_idle is not None
        else StreamGroupSignalSource(stream_spec=spec, poll_interval=_FAST)
    )
    return RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=source,
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
    )


async def _append(ctx, spec, signal: RealtimeSignal, *, event_id: str | None) -> None:  # type: ignore[no-untyped-def]
    cmd = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
    headers = {HEADER_EVENT_ID: event_id} if event_id else {}
    await cmd.append(str(spec.name), signal, type=signal.event, headers=headers)


async def _run_settle(gw: RealtimeGateway, ctx, predicate, *, settle: float = 0.08, timeout: float = 2.0):  # type: ignore[no-untyped-def]
    task = asyncio.create_task(gw.run(ctx))
    try:
        waited = 0.0
        while not predicate() and waited < timeout:
            await asyncio.sleep(0.01)
            waited += 0.01
        await asyncio.sleep(settle)  # let any duplicate get processed (and skipped)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


# ----------------------- #


async def test_durable_duplicate_emits_once() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _deduping_gateway(sio, spec)
    sig = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "shipped"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append(ctx, spec, sig, event_id="evt-1")
        await _append(ctx, spec, sig, event_id="evt-1")  # relay retry / claim duplicate
        await _run_settle(gw, ctx, lambda: bool(sio.emits))

    assert [e["room"] for e in sio.emits] == ["principal:u1"]  # deduped to one


async def test_distinct_durable_signals_both_emit() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _deduping_gateway(sio, spec)
    sig = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "x"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append(ctx, spec, sig, event_id="evt-1")
        await _append(ctx, spec, sig, event_id="evt-2")
        await _run_settle(gw, ctx, lambda: len(sio.emits) >= 2)

    assert len(sio.emits) == 2


async def test_ephemeral_duplicates_are_not_deduped() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _deduping_gateway(sio, spec)
    sig = RealtimeSignal.of(Audience.topic("c"), "typing", {"text": "x"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append(ctx, spec, sig, event_id=None)  # ephemeral — no dedup id
        await _append(ctx, spec, sig, event_id=None)
        await _run_settle(gw, ctx, lambda: len(sio.emits) >= 2)

    assert len(sio.emits) == 2  # ephemeral is at-most-once, never deduped


async def test_durable_emit_failure_is_not_acked() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    sio.fail = True  # every emit raises
    gw = _deduping_gateway(sio, spec)
    sig = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "x"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append(ctx, spec, sig, event_id="evt-1")
        await _run_settle(gw, ctx, lambda: sio.attempts >= 1)

        group = ctx.deps.resolve_configurable(ctx, AckStreamGroupQueryDepKey, spec, route=spec.name)
        pending = await group.pending("realtime-gateway", str(spec.name))

    assert sio.emits == []  # never delivered
    assert pending != []  # left pending → redeliverable (at-least-once)


async def test_durable_transient_emit_failure_is_reclaimed_and_re_emitted() -> None:
    # a non-mailboxed durable signal is emitted INSIDE the dedup transaction, so a failed
    # emit rolls the mark back and the stream entry stays pending — reclaim re-delivers and
    # the retry succeeds. The frame is never lost (at-least-once; the client dedups by id).
    spec = realtime_stream_spec()
    sio = _StubSio()
    sio.fail_times = 1  # first emit fails (mark rolls back, not acked), the retry succeeds
    gw = _deduping_gateway(sio, spec, reclaim_idle=timedelta(0))  # reclaim stranded entries at once
    sig = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "x"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append(ctx, spec, sig, event_id="evt-1")
        await _run_settle(gw, ctx, lambda: bool(sio.emits))

        group = ctx.deps.resolve_configurable(ctx, AckStreamGroupQueryDepKey, spec, route=spec.name)
        pending = await group.pending("realtime-gateway", str(spec.name))

    assert sio.attempts == 2  # one failure, then a successful retry via reclaim
    assert len(sio.emits) == 1  # recovered and emitted — not lost
    assert pending == []  # acked after the successful retry


async def test_end_to_end_durable_stage_relay_gateway() -> None:
    spec = realtime_stream_spec()
    outbox_spec = realtime_outbox_spec()
    sio = _StubSio()
    gw = _deduping_gateway(sio, spec)

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec, outbox_spec=outbox_spec)
        with ctx.inv_ctx.bind_identity(tenant=_TENANT):
            await pub.stage(Audience.principal("u1"), _ORDER_SHIPPED, _MsgView(text="shipped"))
            await ctx.outbox.command(outbox_spec).flush()
            await OutboxRelay(outbox_spec=outbox_spec).to_stream(ctx, spec)
        await _run_settle(gw, ctx, lambda: bool(sio.emits))

    assert len(sio.emits) == 1
    assert sio.emits[0]["event"] == "order.shipped"
    assert sio.emits[0]["room"] == f"t:{_TENANT.tenant_id}:principal:u1"
    # uniform envelope: durable carries its event id + the payload under "data"
    assert sio.emits[0]["data"]["id"] is not None
    assert sio.emits[0]["data"]["data"] == {"text": "shipped"}


async def test_poison_durable_signal_dropped_at_delivery_ceiling() -> None:
    """A durable signal that fails every emit is dropped at max_deliveries, not retried forever."""

    spec = realtime_stream_spec()
    sio = _StubSio()
    sio.fail = True  # every emit fails — a genuine poison signal
    source = StreamGroupSignalSource(
        stream_spec=spec,
        poll_interval=_FAST,
        reclaim_idle=timedelta(0),  # reclaim stranded entries immediately
        max_deliveries=3,
    )
    gw = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=source,
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
    )
    sig = RealtimeSignal.of(Audience.topic("orders"), "order.shipped", {"text": "poison"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append(ctx, spec, sig, event_id="evt-poison")
        # settle long enough for read + reclaim cycles well past the ceiling
        await _run_settle(gw, ctx, lambda: sio.attempts >= 3, settle=0.2)

        group = ctx.deps.resolve_configurable(
            ctx, AckStreamGroupQueryDepKey, spec, route=spec.name
        )
        pending = await group.pending("realtime-gateway", str(spec.name))

    assert sio.attempts == 3  # first read + two reclaims — the ceiling, then dropped
    assert sio.emits == []  # never delivered (bounded loss, not silent success)
    assert pending == []  # acked at the ceiling — no eternal reclaim loop


async def test_healthy_signal_delivers_after_poison_dropped() -> None:
    """Dropping the poison unblocks the lane — a later durable signal still delivers."""

    spec = realtime_stream_spec()
    sio = _StubSio()
    sio.fail_times = 999  # poison-like failure while the first signal is in flight
    source = StreamGroupSignalSource(
        stream_spec=spec,
        poll_interval=_FAST,
        reclaim_idle=timedelta(0),
        max_deliveries=2,
    )
    gw = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=source,
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
    )
    sig = RealtimeSignal.of(Audience.topic("orders"), "order.shipped", {"text": "poison"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append(ctx, spec, sig, event_id="evt-poison")
        await _run_settle(gw, ctx, lambda: sio.attempts >= 2, settle=0.2)

        sio.fail_times = 0  # backend recovered; next signal is healthy
        healthy = RealtimeSignal.of(Audience.topic("orders"), "order.shipped", {"text": "ok"})
        await _append(ctx, spec, healthy, event_id="evt-ok")
        await _run_settle(gw, ctx, lambda: bool(sio.emits))

    assert len(sio.emits) == 1
    assert sio.emits[0]["data"]["data"] == {"text": "ok"}


async def test_crash_between_commit_and_ack_redelivers_exactly_once() -> None:
    """Mark committed + emit done, but never acked (a crash) → reclaim dedups, no double emit."""

    spec = realtime_stream_spec()
    sio = _StubSio()
    source = StreamGroupSignalSource(
        stream_spec=spec, poll_interval=_FAST, reclaim_idle=timedelta(0)
    )
    gw = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=source,
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
    )
    sig = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "once"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append(ctx, spec, sig, event_id="evt-crash")

        # Simulate the crashed gateway: deliver the message to the group (it becomes
        # pending), run the bridge by hand so the dedup mark commits and the emit
        # happens — then "crash" before the ack ever happens.
        group = ctx.deps.resolve_configurable(
            ctx, AckStreamGroupQueryDepKey, spec, route=spec.name
        )
        delivered = await group.read("realtime-gateway", "crashed-node", {str(spec.name): ">"})
        assert len(delivered) == 1
        await gw._handle(  # pyright: ignore[reportPrivateUsage]
            ctx,
            None,
            delivered[0].payload,
            None,
            "evt-crash",
            _hlc_stub(),
        )
        assert len(sio.emits) == 1  # the crashed node did emit before dying

        # A fresh gateway takes over: reclaim redelivers the pending entry, the dedup
        # mark stands, nothing double-emits, and the entry is finally acked.
        await _run_settle(gw, ctx, lambda: False, settle=0.15)

        pending = await group.pending("realtime-gateway", str(spec.name))

    assert len(sio.emits) == 1  # exactly once, across the crash
    assert pending == []  # recovered — the reclaim pass acked it


def _hlc_stub():  # type: ignore[no-untyped-def]
    from forze.base.primitives import HlcTimestamp, utcnow

    return HlcTimestamp(physical_ms=int(utcnow().timestamp() * 1000), logical=0)


async def test_gateway_stats_count_delivery_outcomes() -> None:
    from forze_socketio import RealtimeGatewayStats

    spec = realtime_stream_spec()
    sio = _StubSio()
    stats = RealtimeGatewayStats()
    source = StreamGroupSignalSource(stream_spec=spec, poll_interval=_FAST, stats=stats)
    gw = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=source,
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
        stats=stats,
    )
    sig = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "x"})

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await _append(ctx, spec, sig, event_id="evt-1")
        await _append(ctx, spec, sig, event_id="evt-1")  # duplicate → dedup skip
        await _run_settle(gw, ctx, lambda: stats.emitted >= 1 and stats.dedup_skipped >= 1)

    assert stats.emitted == 1
    assert stats.dedup_skipped == 1
    assert stats.emit_failed == 0


async def test_encrypted_realtime_stream_is_refused_at_run() -> None:
    from typing import cast

    from forze.application.contracts.realtime import RealtimeSignal as _RS
    from forze.application.contracts.stream import StreamSpec
    from forze.application.execution import ExecutionContext
    from forze.base.exceptions import CoreException
    from forze.base.serialization import PydanticModelCodec

    sealed = StreamSpec(
        name="realtime", codec=PydanticModelCodec(model_type=_RS), encryption="end_to_end"
    )
    source = StreamGroupSignalSource(stream_spec=sealed)

    async def _handler(*args: Any) -> None:  # pragma: no cover - never reached
        return

    with pytest.raises(CoreException) as caught:
        await source.run(cast(ExecutionContext, None), _handler)

    assert caught.value.code == "realtime_stream_encryption_unsupported"


async def test_stats_count_mailboxed_emit_failed_and_poisoned() -> None:
    """The stat lines on the failure/store paths — mailboxed, emit_failed, poisoned."""

    from forze_kits.integrations.realtime import build_realtime_mailbox
    from forze_socketio import RealtimeGatewayStats

    spec = realtime_stream_spec()
    sio = _StubSio()
    sio.fail = True  # every emit fails
    stats = RealtimeGatewayStats()
    source = StreamGroupSignalSource(
        stream_spec=spec,
        poll_interval=_FAST,
        reclaim_idle=timedelta(0),
        max_deliveries=3,
        stats=stats,
    )
    gw = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=source,
        dedup=GatewayDedup(inbox_spec=realtime_inbox_spec(), tx_route="mock"),
        mailbox_factory=build_realtime_mailbox,
        stats=stats,
    )

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()

        # A principal signal is mailboxed (mark+store commit), then its live emit fails —
        # best-effort, swallowed: emitted 0, emit_failed 1, mailboxed 1, nothing poisoned.
        principal = RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": "p"})
        await _append(ctx, spec, principal, event_id="00000000-0000-0000-0000-0000000000aa")
        await _run_settle(gw, ctx, lambda: stats.mailboxed >= 1, settle=0.1)

        assert stats.mailboxed == 1
        assert stats.emit_failed >= 1
        assert stats.emitted == 0
        assert stats.poisoned == 0  # recoverable via mailbox — never dropped

        # A topic-durable signal (emit inside the tx) fails every delivery and is dropped
        # at the ceiling: poisoned counted exactly once, only after the ack landed.
        topic = RealtimeSignal.of(Audience.topic("orders"), "order.shipped", {"text": "t"})
        await _append(ctx, spec, topic, event_id="00000000-0000-0000-0000-0000000000bb")
        await _run_settle(gw, ctx, lambda: stats.poisoned >= 1, settle=0.2)

        assert stats.poisoned == 1
        assert stats.bridge_failed >= 2  # the pre-ceiling failures counted as bridge failures


async def test_stats_count_admission_rejections() -> None:
    from forze_socketio import RealtimeGatewayStats

    spec = realtime_stream_spec()
    sio = _StubSio()
    stats = RealtimeGatewayStats()
    catalog = RealtimeEventCatalog.of(
        RealtimeEvent(name="order.shipped", payload_type=_MsgView)
    )
    gw = RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=StreamGroupSignalSource(stream_spec=spec, poll_interval=_FAST, stats=stats),
        event_catalog=catalog,
        stats=stats,
    )

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        undeclared = RealtimeSignal.of(Audience.topic("t"), "not.in.catalog", {"x": 1})
        malformed = RealtimeSignal.of(Audience.topic("t"), "order.shipped", {"bogus": True})
        await _append(ctx, spec, undeclared, event_id=None)
        await _append(ctx, spec, malformed, event_id=None)
        await _run_settle(gw, ctx, lambda: stats.admission_rejected >= 2, settle=0.1)

    assert stats.admission_rejected == 2
    assert sio.emits == []  # rejected at the gate, never emitted
