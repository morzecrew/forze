"""Tests for the realtime publish surface (E2)."""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, AudienceKind, RealtimeEvent, RealtimeSignal
from forze.application.contracts.stream import (
    StreamCommandDepKey,
    StreamGroupQueryDepKey,
    StreamQueryDepKey,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.realtime import (
    RealtimePublisher,
    RealtimeTransport,
    build_realtime_transport,
    realtime_group_ensure_lifecycle_step,
    realtime_outbox_spec,
    realtime_relay_lifecycle_step,
    realtime_stream_spec,
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
_ORDER_SHIPPED = RealtimeEvent(name="order.shipped", payload_type=_MsgView)  # any kind
_TENANT = TenantIdentity(tenant_id=UUID("11111111-1111-1111-1111-111111111111"))


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


def _publisher() -> RealtimePublisher:
    return RealtimePublisher(
        stream_spec=realtime_stream_spec(),
        outbox_spec=realtime_outbox_spec(),
    )


async def _read_stream(ctx, spec):  # type: ignore[no-untyped-def]
    query = ctx.deps.resolve_configurable(ctx, StreamQueryDepKey, spec, route=spec.name)
    return await query.read({str(spec.name): "0"})


# ----------------------- #
# ephemeral publish


async def test_publish_appends_signal_with_tenant_header() -> None:
    rt = _publisher()
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with ctx.inv_ctx.bind_identity(tenant=_TENANT):
            mid = await rt.publish(
                ctx, Audience.topic("chat:42"), _MESSAGE_NEW, _MsgView(text="hi")
            )
        rows = await _read_stream(ctx, rt.stream_spec)

    assert mid
    [msg] = rows
    assert msg.type == "message.new"
    assert msg.key == "topic:chat:42"  # per-audience partition key keeps a topic ordered
    assert msg.payload.event == "message.new"
    assert msg.payload.audience == Audience.topic("chat:42")
    assert msg.payload.payload == {"text": "hi"}
    # tenant rides in the headers (channel stays tenant-global)
    assert dict(msg.headers) == {"forze_tenant_id": str(_TENANT.tenant_id)}


async def test_publish_without_tenant_has_no_tenant_header() -> None:
    rt = _publisher()
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await rt.publish(ctx, Audience.topic("c"), _MESSAGE_NEW, _MsgView(text="x"))
        rows = await _read_stream(ctx, rt.stream_spec)

    assert dict(rows[0].headers) == {}


async def test_publish_refused_in_read_only_operation() -> None:
    rt = _publisher()
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with ctx.inv_ctx.bind_read_only():
            with pytest.raises(CoreException) as err:
                await rt.publish(ctx, Audience.topic("c"), _MESSAGE_NEW, _MsgView(text="x"))

    assert err.value.kind.value == "precondition"


async def test_publish_enforces_audience_constraint() -> None:
    rt = _publisher()
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        # message.new is topic-only; a principal target is rejected
        with pytest.raises(CoreException) as err:
            await rt.publish(ctx, Audience.principal("u"), _MESSAGE_NEW, _MsgView(text="x"))

    assert err.value.kind.value == "precondition"


# ----------------------- #
# durable stage


async def test_stage_then_relay_reaches_stream() -> None:
    rt = _publisher()
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with ctx.inv_ctx.bind_identity(tenant=_TENANT):
            await rt.stage(
                ctx, Audience.principal("u1"), _ORDER_SHIPPED, _MsgView(text="shipped")
            )
            assert await ctx.outbox.command(rt.outbox_spec).flush() == 1

            result = await OutboxRelay(outbox_spec=rt.outbox_spec).to_stream(
                ctx, rt.stream_spec
            )
        rows = await _read_stream(ctx, rt.stream_spec)

    assert result.published == 1
    [msg] = rows
    assert msg.payload.event == "order.shipped"
    assert msg.payload.audience == Audience.principal("u1")
    # the relay carries the tenant captured at stage time
    assert dict(msg.headers).get("forze_tenant_id") == str(_TENANT.tenant_id)


async def test_stage_requires_outbox_spec() -> None:
    rt = RealtimePublisher(stream_spec=realtime_stream_spec())  # no outbox_spec
    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        with pytest.raises(CoreException) as err:
            await rt.stage(ctx, Audience.topic("c"), _MESSAGE_NEW, _MsgView(text="x"))

    assert err.value.kind.value == "configuration"


async def test_relay_lifecycle_step_moves_staged_signal_to_stream() -> None:
    spec = realtime_stream_spec()
    outbox_spec = realtime_outbox_spec()
    rt = RealtimePublisher(stream_spec=spec, outbox_spec=outbox_spec)
    step = realtime_relay_lifecycle_step(
        outbox_spec=outbox_spec, stream_spec=spec, interval=timedelta(seconds=0.02)
    )

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await rt.stage(ctx, Audience.principal("u1"), _ORDER_SHIPPED, _MsgView(text="x"))
        await ctx.outbox.command(outbox_spec).flush()

        await step.startup(ctx)
        rows: list = []
        for _ in range(200):
            await asyncio.sleep(0.01)
            rows = await _read_stream(ctx, spec)
            if rows:
                break
        await step.shutdown(ctx)

    assert len(rows) == 1
    assert rows[0].payload.event == "order.shipped"
    assert rows[0].payload.audience == Audience.principal("u1")


# ----------------------- #
# transport bundle + group-ensure step


def test_build_realtime_transport_derives_consistent_specs() -> None:
    t = build_realtime_transport("chat")

    assert isinstance(t, RealtimeTransport)
    assert str(t.stream_spec.name) == "chat"
    assert str(t.outbox_spec.name) == "chat"
    assert str(t.inbox_spec.name) == "chat-inbox"  # inbox derived from the channel
    # the outbox relays to the same channel the stream consumes
    assert t.outbox_spec.destination is not None
    assert t.outbox_spec.destination.channel == "chat"


async def test_group_ensure_step_skips_backlog_and_is_idempotent() -> None:
    spec = realtime_stream_spec()
    step = realtime_group_ensure_lifecycle_step(stream_spec=spec, group="gw")  # start_id="$"

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        cmd = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)

        # a signal published BEFORE the group is created
        await cmd.append(str(spec.name), RealtimeSignal.of(Audience.topic("a"), "e", {"x": 1}))

        await step.startup(ctx)
        await step.startup(ctx)  # idempotent — no error

        group = ctx.deps.resolve_configurable(ctx, StreamGroupQueryDepKey, spec, route=spec.name)
        backlog = await group.read("gw", "c", {str(spec.name): ">"})

        # "$" delivers only what arrives after creation
        await cmd.append(str(spec.name), RealtimeSignal.of(Audience.topic("b"), "e", {"x": 2}))
        fresh = await group.read("gw", "c", {str(spec.name): ">"})

    assert backlog == []  # the pre-creation signal is skipped
    assert [m.payload.audience for m in fresh] == [Audience.topic("b")]
