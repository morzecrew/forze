"""Durable on-ramp + gateway exactly-once dedup (E4)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_EVENT_ID
from forze.application.contracts.realtime import Audience, RealtimeEvent, RealtimeSignal
from forze.application.contracts.stream import StreamCommandDepKey, StreamGroupQueryDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.realtime import (
    build_realtime_publisher,
    realtime_inbox_spec,
    realtime_outbox_spec,
    realtime_stream_spec,
)
from forze_socketio import GatewayDedup, RealtimeGateway, StreamGroupSignalSource
from forze_mock import MockDepsModule

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

        group = ctx.deps.resolve_configurable(ctx, StreamGroupQueryDepKey, spec, route=spec.name)
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

        group = ctx.deps.resolve_configurable(ctx, StreamGroupQueryDepKey, spec, route=spec.name)
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
