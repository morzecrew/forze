"""Tests for the realtime egress gateway (E3)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, AudienceKind, RealtimeEvent
from forze.application.contracts.stream import StreamGroupQueryDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_kits.integrations.realtime import build_realtime_publisher, realtime_stream_spec
from forze_socketio import (
    RealtimeGateway,
    StreamGroupSignalSource,
    realtime_gateway_lifecycle_step,
    room_for,
)
from forze_mock import MockDepsModule

# ----------------------- #


class _MsgView(BaseModel):
    text: str


_MESSAGE_NEW = RealtimeEvent(
    name="message.new",
    payload_type=_MsgView,
    audience_kinds=frozenset({AudienceKind.TOPIC}),
)
_TENANT = TenantIdentity(tenant_id=UUID("11111111-1111-1111-1111-111111111111"))
_FAST = timedelta(seconds=0.01)


class _StubSio:
    """Records emits and room membership operations."""

    def __init__(self) -> None:
        self.emits: list[dict[str, Any]] = []
        self.entered: list[tuple[str, str]] = []
        self.left: list[tuple[str, str]] = []
        self.fail_first_emit = False

    async def emit(self, event: str, data: Any = None, *, namespace: str | None = None,
                   room: str | None = None, **_: Any) -> None:
        if self.fail_first_emit:
            self.fail_first_emit = False
            raise RuntimeError("boom")
        self.emits.append({"event": event, "data": data, "room": room, "namespace": namespace})

    async def enter_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.entered.append((sid, room))

    async def leave_room(self, sid: str, room: str, namespace: str | None = None) -> None:
        self.left.append((sid, room))


def _gateway(sio: _StubSio, spec) -> RealtimeGateway:  # type: ignore[no-untyped-def]
    return RealtimeGateway(
        sio=sio,  # pyright: ignore[reportArgumentType]
        source=StreamGroupSignalSource(stream_spec=spec, poll_interval=_FAST),
    )


async def _run_until(gw: RealtimeGateway, ctx, predicate, *, timeout: float = 2.0) -> None:  # type: ignore[no-untyped-def]
    task = asyncio.create_task(gw.run(ctx))
    try:
        waited = 0.0
        while not predicate() and waited < timeout:
            await asyncio.sleep(0.01)
            waited += 0.01
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# ----------------------- #
# room_for


def test_room_for_scoped_and_unscoped() -> None:
    assert room_for(Audience.topic("chat"), _TENANT.tenant_id) == f"t:{_TENANT.tenant_id}:topic:chat"
    assert room_for(Audience.principal("u-1"), _TENANT.tenant_id) == f"t:{_TENANT.tenant_id}:principal:u-1"
    assert room_for(Audience.topic("chat"), None) == "topic:chat"


# ----------------------- #
# end-to-end ephemeral: publish -> gateway -> emit


async def test_gateway_emits_published_signal_to_tenant_room() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _gateway(sio, spec)

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec)
        with ctx.inv_ctx.bind_identity(tenant=_TENANT):
            await pub.publish(Audience.topic("chat:42"), _MESSAGE_NEW, _MsgView(text="hi"))
        await _run_until(gw, ctx, lambda: bool(sio.emits))

    assert sio.emits == [
        {
            "event": "message.new",
            # uniform envelope: ephemeral carries id=None
            "data": {"id": None, "data": {"text": "hi"}},
            "room": f"t:{_TENANT.tenant_id}:topic:chat:42",
            "namespace": "/",
        }
    ]


async def test_gateway_emits_unscoped_when_no_tenant() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _gateway(sio, spec)

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec)
        await pub.publish(Audience.topic("c"), _MESSAGE_NEW, _MsgView(text="x"))
        await _run_until(gw, ctx, lambda: bool(sio.emits))

    assert sio.emits[0]["room"] == "topic:c"


async def test_signal_is_acked_after_emit() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _gateway(sio, spec)

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec)
        await pub.publish(Audience.topic("c"), _MESSAGE_NEW, _MsgView(text="x"))
        await _run_until(gw, ctx, lambda: bool(sio.emits))

        group = ctx.deps.resolve_configurable(ctx, StreamGroupQueryDepKey, spec, route=spec.name)
        pending = await group.pending("realtime-gateway", str(spec.name))

    assert pending == []  # acknowledged → nothing left pending


async def test_bridge_error_is_isolated_and_acked() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    sio.fail_first_emit = True  # first emit raises; loop must keep going + ack
    gw = _gateway(sio, spec)

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec)
        await pub.publish(Audience.topic("a"), _MESSAGE_NEW, _MsgView(text="1"))
        await pub.publish(Audience.topic("b"), _MESSAGE_NEW, _MsgView(text="2"))
        await _run_until(gw, ctx, lambda: bool(sio.emits))

        group = ctx.deps.resolve_configurable(ctx, StreamGroupQueryDepKey, spec, route=spec.name)
        pending = await group.pending("realtime-gateway", str(spec.name))

    # the failed signal was still acked (not wedged), the second one emitted
    assert [e["room"] for e in sio.emits] == ["topic:b"]
    assert pending == []


# ----------------------- #
# membership


async def test_membership_join_and_leave() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _gateway(sio, spec)

    await gw.join_principal("sid-1", _TENANT.tenant_id, _TENANT.tenant_id)
    await gw.join_topic("sid-1", "chat:9", _TENANT.tenant_id)
    await gw.leave_topic("sid-1", "chat:9", _TENANT.tenant_id)

    assert sio.entered == [
        ("sid-1", f"t:{_TENANT.tenant_id}:principal:{_TENANT.tenant_id}"),
        ("sid-1", f"t:{_TENANT.tenant_id}:topic:chat:9"),
    ]
    assert sio.left == [("sid-1", f"t:{_TENANT.tenant_id}:topic:chat:9")]


# ----------------------- #
# lifecycle step


async def test_lifecycle_step_runs_and_stops() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = _gateway(sio, spec)
    step = realtime_gateway_lifecycle_step(gw)

    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec)
        await pub.publish(Audience.topic("c"), _MESSAGE_NEW, _MsgView(text="x"))

        await step.startup(ctx)
        waited = 0.0
        while not sio.emits and waited < 2.0:
            await asyncio.sleep(0.01)
            waited += 0.01
        await step.shutdown(ctx)

    assert len(sio.emits) == 1
    assert step.startup.task is not None and step.startup.task.cancelled()
