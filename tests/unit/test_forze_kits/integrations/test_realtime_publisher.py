"""Tests for the realtime publish surface (E2)."""

from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.realtime import Audience, AudienceKind, RealtimeEvent
from forze.application.contracts.stream import StreamQueryDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze_kits.integrations.outbox import OutboxRelay
from forze_kits.integrations.realtime import (
    RealtimePublisher,
    realtime_outbox_spec,
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
