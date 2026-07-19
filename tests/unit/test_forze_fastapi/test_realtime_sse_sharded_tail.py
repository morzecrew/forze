"""The tenant-sharded SSE tail — namespace-tier per-tenant streams, trusted tenant.

# covers: forze_fastapi.realtime.lifecycle (_sharded_tail_to_hub, per-tenant binding,
#         header distrust, empty shard idle, unassigned-tenant isolation,
#         realtime_sse_sharded_tail_lifecycle_step)

The tenant-global tail trusts the ``forze_tenant_id`` header; here the stream route is
``tenant_aware`` and each loop binds its shard tenant, so the tenant a signal fans out
under is the **stream it was read from** — a forged header must be ignored, and a
tenant outside the shard must never reach the hub.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.envelope import HEADER_TENANT_ID
from forze.application.contracts.realtime import (
    Audience,
    RealtimeEvent,
    RealtimeShard,
    RealtimeSignal,
)
from forze.application.contracts.stream import StreamCommandDepKey
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze_fastapi.realtime import (
    RealtimeSseHub,
    realtime_sse_sharded_tail_lifecycle_step,
)
from forze_fastapi.realtime.lifecycle import (
    _sharded_tail_to_hub,  # pyright: ignore[reportPrivateUsage]
)
from forze_kits.integrations.realtime import realtime_stream_spec
from forze_mock import MockDepsModule, MockRouteConfig

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")
_T3 = UUID("33333333-3333-3333-3333-333333333333")
_FAST = timedelta(milliseconds=10)


class _View(BaseModel):
    n: int


_EVENT = RealtimeEvent(name="e", payload_type=_View)


def _runtime() -> ExecutionRuntime:
    spec = realtime_stream_spec()
    module = MockDepsModule(routes={str(spec.name): MockRouteConfig(tenant_aware=True)})

    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


async def _append_for(
    ctx: ExecutionContext,
    tenant: UUID,
    signal: RealtimeSignal,
    *,
    headers: dict[str, str] | None = None,
) -> None:
    """Append onto *tenant*'s stream (the producer's ambient tenant picks the key)."""

    spec = realtime_stream_spec()

    with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
        port = ctx.deps.resolve_configurable(ctx, StreamCommandDepKey, spec, route=spec.name)
        await port.append(str(spec.name), signal, type=signal.event, headers=headers or {})


def _signal(n: int) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.topic("room"), "e", {"n": n})


async def _run_shard(
    ctx: ExecutionContext, hub: RealtimeSseHub, tenants: list[UUID], stop: asyncio.Event
) -> asyncio.Task[None]:
    return asyncio.create_task(
        _sharded_tail_to_hub(
            ctx,
            hub=hub,
            shard=RealtimeShard(stream_spec=realtime_stream_spec(), tenants=tuple(tenants)),
            batch=16,
            poll_interval=_FAST,
            restart_backoff=_FAST,
            max_consecutive_crashes=None,
            stop=stop,
        )
    )


async def _drain(sub: Any, *, timeout: float = 5.0) -> tuple[RealtimeSignal, str | None]:
    return await asyncio.wait_for(sub.queue.get(), timeout=timeout)


# ----------------------- #


class TestShardedTail:
    async def test_each_tenant_receives_under_its_stream_identity(self) -> None:
        hub = RealtimeSseHub()
        sub1 = hub.subscribe(principal="nobody", tenant=_T1, topics=frozenset({"room"}))
        sub2 = hub.subscribe(principal="nobody", tenant=_T2, topics=frozenset({"room"}))
        stop = asyncio.Event()

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            task = await _run_shard(ctx, hub, [_T1, _T2], stop)
            await asyncio.sleep(0.2)  # both tenant loops past fast-forward

            await _append_for(ctx, _T1, _signal(1))
            await _append_for(ctx, _T2, _signal(2))

            got1, _ = await _drain(sub1)
            got2, _ = await _drain(sub2)
            assert got1.payload == {"n": 1}
            assert got2.payload == {"n": 2}
            assert sub1.queue.empty() and sub2.queue.empty()  # no cross-tenant bleed

            stop.set()
            await asyncio.wait_for(task, timeout=5)

    async def test_forged_header_is_ignored_the_stream_identity_wins(self) -> None:
        hub = RealtimeSseHub()
        sub1 = hub.subscribe(principal="nobody", tenant=_T1, topics=frozenset({"room"}))
        sub2 = hub.subscribe(principal="nobody", tenant=_T2, topics=frozenset({"room"}))
        stop = asyncio.Event()

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            task = await _run_shard(ctx, hub, [_T1, _T2], stop)
            await asyncio.sleep(0.2)

            # written to T1's stream but claiming T2 in the header — the header loses
            await _append_for(ctx, _T1, _signal(1), headers={HEADER_TENANT_ID: str(_T2)})

            got, _ = await _drain(sub1)
            assert got.payload == {"n": 1}
            assert sub2.queue.empty()

            stop.set()
            await asyncio.wait_for(task, timeout=5)

    async def test_unassigned_tenant_is_never_consumed(self) -> None:
        hub = RealtimeSseHub()
        sub3 = hub.subscribe(principal="nobody", tenant=_T3, topics=frozenset({"room"}))
        stop = asyncio.Event()

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            task = await _run_shard(ctx, hub, [_T1], stop)  # T3 not in the shard
            await asyncio.sleep(0.2)

            await _append_for(ctx, _T3, _signal(3))
            await asyncio.sleep(0.2)
            assert sub3.queue.empty()  # nobody tails T3's stream on this node

            stop.set()
            await asyncio.wait_for(task, timeout=5)

    async def test_fast_forward_skips_each_tenants_backlog(self) -> None:
        hub = RealtimeSseHub()
        sub1 = hub.subscribe(principal="nobody", tenant=_T1, topics=frozenset({"room"}))
        stop = asyncio.Event()

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            await _append_for(ctx, _T1, _signal(0))  # pre-startup backlog

            task = await _run_shard(ctx, hub, [_T1], stop)
            await asyncio.sleep(0.2)
            assert sub1.queue.empty()

            await _append_for(ctx, _T1, _signal(1))
            got, _ = await _drain(sub1)
            assert got.payload == {"n": 1}

            stop.set()
            await asyncio.wait_for(task, timeout=5)

    async def test_empty_shard_idles_until_stopped(self) -> None:
        stop = asyncio.Event()

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            task = await _run_shard(ctx, RealtimeSseHub(), [], stop)

            await asyncio.sleep(0.05)
            assert not task.done()  # an early return would look like a crash upstream

            stop.set()
            await asyncio.wait_for(task, timeout=5)


class TestLifecycleStep:
    async def test_startup_supervises_and_shutdown_stops(self) -> None:
        hub = RealtimeSseHub()
        step = realtime_sse_sharded_tail_lifecycle_step(
            hub,
            shard=RealtimeShard(stream_spec=realtime_stream_spec(), tenants=(_T1,)),
            poll_interval=_FAST,
        )

        runtime = _runtime()
        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)
            first = step.startup.task  # type: ignore[attr-defined]
            assert first is not None and not first.done()
            assert step.startup.loop_name == "realtime_sse_sharded_tail"  # type: ignore[attr-defined]

            await step.startup(ctx)  # duplicate startup must not orphan the running task
            assert step.startup.task is first  # type: ignore[attr-defined]

            await step.shutdown(ctx)
            assert first.done()

    def test_invalid_settings_are_refused(self) -> None:
        shard = RealtimeShard(stream_spec=realtime_stream_spec(), tenants=(_T1,))

        for kwargs in (
            {"batch": 0},
            {"poll_interval": timedelta(0)},
            {"restart_backoff": timedelta(0)},
            {"max_consecutive_crashes": 0},
        ):
            with pytest.raises(CoreException):
                realtime_sse_sharded_tail_lifecycle_step(
                    RealtimeSseHub(), shard=shard, **kwargs
                )
