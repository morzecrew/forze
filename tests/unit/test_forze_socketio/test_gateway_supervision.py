"""Supervision and drain behavior of the realtime gateway loops.

# covers: forze_socketio.gateway_lifecycle, forze_socketio.gateway (sharded isolation)

The properties the durable plane already has and the gateway now shares: the loop registers
in ``ctx.drainables`` and stops at a batch boundary (not a mid-emit cancel), a crashed source
restarts after backoff instead of staying dead, and in the tenant-sharded source one tenant's
failure degrades that tenant only.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.realtime import (
    Audience,
    AudienceKind,
    RealtimeEvent,
    RealtimeShard,
)
from forze.application.execution import ExecutionContext, ExecutionRuntime
from forze.application.execution.deps import DepsRegistry
from forze_kits.integrations.realtime import build_realtime_publisher, realtime_stream_spec
from forze_mock import MockDepsModule
from forze_socketio import (
    RealtimeGateway,
    StreamGroupSignalSource,
    TenantShardedSignalSource,
    realtime_gateway_lifecycle_step,
)
from forze_socketio.gateway import SignalHandler

# ----------------------- #


class _MsgView(BaseModel):
    text: str


_MESSAGE_NEW = RealtimeEvent(
    name="message.new",
    payload_type=_MsgView,
    audience_kinds=frozenset({AudienceKind.TOPIC}),
)


class _StubSio:
    """Records emits."""

    def __init__(self) -> None:
        self.emits: list[dict[str, Any]] = []

    async def emit(
        self,
        event: str,
        data: Any = None,
        *,
        namespace: str | None = None,
        room: str | None = None,
        **_: Any,
    ) -> None:
        self.emits.append({"event": event, "data": data, "room": room, "namespace": namespace})


# ----------------------- #


def _runtime() -> ExecutionRuntime:
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())


# ----------------------- #


async def test_gateway_registers_in_drainables_and_stops_cleanly() -> None:
    spec = realtime_stream_spec()
    sio = _StubSio()
    gw = RealtimeGateway(sio=sio, source=StreamGroupSignalSource(stream_spec=spec))  # type: ignore[arg-type]
    step = realtime_gateway_lifecycle_step(gw)

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        assert step.startup in ctx.drainables.loops

        stopped = await ctx.drainables.stop_all(grace=5.0)
        assert stopped == 1

        task = step.startup.task
        assert task is not None and task.done() and not task.cancelled()


# ....................... #


async def test_gateway_restarts_after_source_crash() -> None:
    """A crashed consume loop is restarted by supervision — delivery does not stay down."""

    spec = realtime_stream_spec()
    sio = _StubSio()
    attempts = {"n": 0}

    inner = StreamGroupSignalSource(stream_spec=spec)

    @attrs.define(slots=True)
    class _CrashyOnce:
        """Crash the first run; delegate to the real source afterwards."""

        async def run(
            self,
            ctx: ExecutionContext,
            handler: SignalHandler,
            *,
            stop: asyncio.Event | None = None,
        ) -> None:
            attempts["n"] += 1

            if attempts["n"] == 1:
                raise RuntimeError("broker hiccup")

            await inner.run(ctx, handler, stop=stop)

    gw = RealtimeGateway(sio=sio, source=_CrashyOnce())  # type: ignore[arg-type]
    step = realtime_gateway_lifecycle_step(gw, restart_backoff=timedelta(milliseconds=10))

    runtime = _runtime()
    async with runtime.scope():
        ctx = runtime.get_context()
        pub = build_realtime_publisher(ctx, stream_spec=spec)
        await pub.publish(Audience.topic("c"), _MESSAGE_NEW, _MsgView(text="x"))

        await step.startup(ctx)

        waited = 0.0
        while not sio.emits and waited < 5.0:
            await asyncio.sleep(0.01)
            waited += 0.01

        await step.shutdown(ctx)

    assert attempts["n"] >= 2  # first run crashed, supervision restarted it
    assert len(sio.emits) == 1  # and the restarted run delivered


# ....................... #


async def test_sharded_tenant_crash_does_not_stop_siblings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One tenant's broken loop keeps crashing; the sibling tenant keeps consuming."""

    tenant_a, tenant_b = uuid4(), uuid4()
    spec = realtime_stream_spec()
    shard = RealtimeShard(stream_spec=spec, tenants=(tenant_a, tenant_b), group="g")
    source = TenantShardedSignalSource(shard=shard, restart_backoff=timedelta(milliseconds=5))

    crashes = {"n": 0}
    b_ran = asyncio.Event()
    original = TenantShardedSignalSource._run_tenant

    async def _run_tenant(
        self: TenantShardedSignalSource,
        ctx: ExecutionContext,
        tenant: Any,
        handler: SignalHandler,
        stop: asyncio.Event,
    ) -> None:
        if tenant == tenant_a:
            crashes["n"] += 1
            raise RuntimeError("tenant A backend down")

        b_ran.set()
        await original(self, ctx, tenant, handler, stop)

    monkeypatch.setattr(TenantShardedSignalSource, "_run_tenant", _run_tenant)

    async def _handler(*args: Any) -> None:  # pragma: no cover - no signals published
        return

    stop = asyncio.Event()
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        run = asyncio.create_task(source.run(ctx, _handler, stop=stop))

        await asyncio.wait_for(b_ran.wait(), timeout=5)  # B is consuming...

        waited = 0.0
        while crashes["n"] < 3 and waited < 5.0:  # ...while A crashed repeatedly
            await asyncio.sleep(0.01)
            waited += 0.01

        assert crashes["n"] >= 3
        assert not run.done()  # the shard as a whole never went down

        stop.set()
        await asyncio.wait_for(run, timeout=5)  # and a stop ends every loop cleanly
