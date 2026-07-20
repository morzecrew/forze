"""The mailbox retention lifecycle step — periodic, supervised, drainable.

# covers: forze_kits.integrations.realtime.realtime_mailbox_retention_lifecycle_step

The sweep semantics themselves (age deletion, stale-cursor pruning) are asserted in
``test_realtime_mailbox.py``; this proves the step drives them on an interval, registers
as a drainable, isolates per-tenant failures, treats configuration errors as terminal,
and stops between ticks instead of being cancelled mid-sweep.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest

from forze.application.contracts.realtime import Audience, RealtimeSignal
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import HlcTimestamp, utcnow
from forze_kits.integrations.realtime import (
    build_realtime_cursors,
    build_realtime_mailbox,
    realtime_cursor_spec,
    realtime_mailbox_retention_lifecycle_step,
    realtime_mailbox_spec,
)
from forze_kits.integrations.realtime.mailbox import DocumentRealtimeMailbox
from forze_mock.execution import MockDepsModule, MockRouteConfig

# ----------------------- #

_T1 = UUID("11111111-1111-1111-1111-111111111111")
_T2 = UUID("22222222-2222-2222-2222-222222222222")

_FAST = timedelta(milliseconds=10)
_MAX_AGE = timedelta(hours=1)


def _hlc(physical_ms: int) -> HlcTimestamp:
    return HlcTimestamp(physical_ms=physical_ms, logical=0)


def _now_hlc() -> HlcTimestamp:
    return _hlc(int(utcnow().timestamp() * 1000))


def _signal(text: str) -> RealtimeSignal:
    return RealtimeSignal.of(Audience.principal("u1"), "order.shipped", {"text": text})


def _eid(n: int) -> str:
    return str(UUID(int=n))


def _runtime(*, tenant_aware: bool = False) -> ExecutionRuntime:
    routes = (
        {
            str(realtime_mailbox_spec().name): MockRouteConfig(tenant_aware=True),
            str(realtime_cursor_spec().name): MockRouteConfig(tenant_aware=True),
        }
        if tenant_aware
        else {}
    )
    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze())


def _bind(ctx: ExecutionContext, tenant: UUID):  # type: ignore[no-untyped-def]
    return ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


async def _seed_ancient_and_fresh(ctx: ExecutionContext) -> None:
    mailbox = build_realtime_mailbox(ctx)
    await mailbox.store(principal="u1", event_id=_eid(1), hlc=_hlc(1), signal=_signal("old"))
    await mailbox.store(principal="u1", event_id=_eid(2), hlc=_now_hlc(), signal=_signal("new"))


async def _wait_until(check, timeout: float = 5.0) -> bool:  # type: ignore[no-untyped-def]
    waited = 0.0

    while waited < timeout:
        if await check():
            return True

        await asyncio.sleep(0.01)
        waited += 0.01

    return False


# ----------------------- #


async def test_step_sweeps_on_an_interval_and_stops_cleanly() -> None:
    step = realtime_mailbox_retention_lifecycle_step(
        max_age=_MAX_AGE, interval=_FAST, jitter=0.0
    )
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        await _seed_ancient_and_fresh(ctx)
        cursors = build_realtime_cursors(ctx)
        await cursors.advance(principal="u1", client_key="d1", up_to=_now_hlc())

        await step.startup(ctx)
        await step.startup(ctx)  # duplicate startup is ignored while running

        assert step.startup in ctx.drainables.loops  # type: ignore[comparison-overlap]
        assert step.startup.loop_name == "realtime_mailbox_retention"  # type: ignore[attr-defined]

        mailbox = build_realtime_mailbox(ctx)

        async def _ancient_gone() -> bool:
            retained = await mailbox.read_since(principal="u1", since=None)
            return [e.event_id for e in retained] == [_eid(2)]

        assert await _wait_until(_ancient_gone)  # the ancient entry aged out
        assert await cursors.get(principal="u1", client_key="d1") is not None  # fresh survives

        await step.shutdown(ctx)

        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None and task.done() and not task.cancelled()  # stopped between ticks


async def test_step_sweeps_each_assigned_tenant() -> None:
    step = realtime_mailbox_retention_lifecycle_step(
        max_age=_MAX_AGE, interval=_FAST, jitter=0.0, tenants=lambda: (_T1, _T2)
    )
    runtime = _runtime(tenant_aware=True)

    async with runtime.scope():
        ctx = runtime.get_context()

        for tenant in (_T1, _T2):
            with _bind(ctx, tenant):
                await _seed_ancient_and_fresh(ctx)

        await step.startup(ctx)

        async def _both_swept() -> bool:
            for tenant in (_T1, _T2):
                with _bind(ctx, tenant):
                    retained = await build_realtime_mailbox(ctx).read_since(
                        principal="u1", since=None
                    )

                    if [e.event_id for e in retained] != [_eid(2)]:
                        return False

            return True

        assert await _wait_until(_both_swept)  # each tenant's collection aged out

        await step.shutdown(ctx)


@pytest.mark.parametrize(
    "error",
    [RuntimeError("store blip"), exc.infrastructure("store down")],
    ids=["unhandled", "core-operational"],
)
async def test_operational_error_does_not_stop_the_loop(
    monkeypatch,  # type: ignore[no-untyped-def]
    error: Exception,
) -> None:
    calls = {"n": 0}

    async def _flaky(self: Any, *, cutoff: Any) -> int:
        calls["n"] += 1

        if calls["n"] <= 3:  # a bounded burst — after it, quiet ticks (cheap teardown)
            raise error

        return 0

    monkeypatch.setattr(DocumentRealtimeMailbox, "sweep_older_than", _flaky)

    step = realtime_mailbox_retention_lifecycle_step(
        max_age=_MAX_AGE, interval=_FAST, jitter=0.0
    )
    runtime = _runtime()

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        async def _kept_sweeping() -> bool:
            return calls["n"] >= 4  # ticks continued past every failure

        assert await _wait_until(_kept_sweeping)

        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None and not task.done()  # an operational fault never stops it

        await step.shutdown(ctx)
        assert task.done() and not task.cancelled()


@pytest.mark.parametrize(
    "error",
    [RuntimeError("store blip"), exc.infrastructure("store down")],
    ids=["unhandled", "core-operational"],
)
async def test_one_tenants_failure_does_not_starve_the_others(
    monkeypatch,  # type: ignore[no-untyped-def]
    error: Exception,
) -> None:
    broken, healthy = _T1, _T2
    original = DocumentRealtimeMailbox.sweep_older_than
    calls = {"n": 0}

    async def _per_tenant(self: Any, *, cutoff: Any) -> int:
        # the step iterates the assigned tenants in declaration order, one sweep
        # each per tick — even calls are the broken tenant, odd the healthy one
        index = calls["n"]
        calls["n"] += 1

        if index % 2 == 0:
            raise error

        return await original(self, cutoff=cutoff)

    monkeypatch.setattr(DocumentRealtimeMailbox, "sweep_older_than", _per_tenant)

    step = realtime_mailbox_retention_lifecycle_step(
        max_age=_MAX_AGE, interval=_FAST, jitter=0.0, tenants=lambda: (broken, healthy)
    )
    runtime = _runtime(tenant_aware=True)

    async with runtime.scope():
        ctx = runtime.get_context()

        for tenant in (broken, healthy):
            with _bind(ctx, tenant):
                await _seed_ancient_and_fresh(ctx)

        await step.startup(ctx)

        async def _healthy_swept() -> bool:
            with _bind(ctx, healthy):
                retained = await build_realtime_mailbox(ctx).read_since(
                    principal="u1", since=None
                )

                return [e.event_id for e in retained] == [_eid(2)]

        assert await _wait_until(_healthy_swept)  # the sibling tenant kept its retention

        with _bind(ctx, broken):
            retained = await build_realtime_mailbox(ctx).read_since(principal="u1", since=None)
            assert len(retained) == 2  # the broken tenant's sweep never landed

        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None and not task.done()  # isolated, not terminal

        await step.shutdown(ctx)


@pytest.mark.parametrize("tenants", [None, (_T1,)], ids=["tenant-global", "per-tenant"])
async def test_configuration_error_stops_the_loop(
    monkeypatch,  # type: ignore[no-untyped-def]
    tenants: tuple[UUID, ...] | None,
) -> None:
    async def _config_error(self: Any, *, cutoff: Any) -> int:
        raise exc.configuration("route is not wired")

    monkeypatch.setattr(DocumentRealtimeMailbox, "sweep_older_than", _config_error)

    step = realtime_mailbox_retention_lifecycle_step(
        max_age=_MAX_AGE,
        interval=_FAST,
        jitter=0.0,
        tenants=(lambda: tenants) if tenants is not None else None,
    )
    runtime = _runtime(tenant_aware=tenants is not None)

    async with runtime.scope():
        ctx = runtime.get_context()
        await step.startup(ctx)

        task = step.startup.task  # type: ignore[attr-defined]
        assert task is not None

        async def _stopped() -> bool:
            return task.done()

        # wiring does not fix itself: the loop stopped on its own, loudly, not cancelled
        assert await _wait_until(_stopped)
        assert not task.cancelled()

        await step.shutdown(ctx)


def test_invalid_jitter_is_refused() -> None:
    with pytest.raises(CoreException, match="Jitter"):
        realtime_mailbox_retention_lifecycle_step(max_age=_MAX_AGE, jitter=1.5)
