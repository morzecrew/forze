"""Lifecycle steps run with no tenant bound, so they refuse a tenant-scoped port at startup.

A tenant-aware port refuses every call made outside a tenant with ``tenant_required``, an
authentication error no retry clears: a step over one failed at startup with an error that
named no wiring, or started and failed on every tick. Each step below now refuses it once, at
startup, as a configuration error.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import DepsRegistry, ExecutionContext, ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.lifecycle import (
    LeadershipSpec,
    Term,
    leadership_lifecycle_step,
    singleton_lifecycle_step,
)
from forze_mock import MockDepsModule, MockRouteConfig

# ----------------------- #

LOCK = DistributedLockSpec(name="lifecycle-lock")


def _runtime(*, tenant_aware: bool) -> ExecutionRuntime:
    routes = {str(LOCK.name): MockRouteConfig(tenant_aware=tenant_aware)}

    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze())


def _refused(error: CoreException) -> None:
    assert error.kind is ExceptionKind.CONFIGURATION
    assert error.code == "tenant_global_required"


# ....................... #


class TestSingleton:
    @staticmethod
    def _step(ran: list[str]) -> LifecycleStep:
        async def startup(ctx: ExecutionContext) -> None:
            ran.append("startup")

        return singleton_lifecycle_step(
            LifecycleStep(id="ensure", startup=startup), spec=LOCK, owner="replica-a"
        )

    async def test_a_tenant_aware_lock_is_refused_at_startup(self) -> None:
        ran: list[str] = []
        step = self._step(ran)

        runtime = _runtime(tenant_aware=True)

        async with runtime.scope():
            with pytest.raises(CoreException) as caught:
                await step.startup(runtime.get_context())

        _refused(caught.value)
        assert ran == []

    async def test_a_tenant_global_lock_runs_the_step(self) -> None:
        ran: list[str] = []
        step = self._step(ran)

        runtime = _runtime(tenant_aware=False)

        async with runtime.scope():
            await step.startup(runtime.get_context())

        assert ran == ["startup"]


# ....................... #


class TestLeadership:
    @staticmethod
    def _step(led: asyncio.Event) -> LifecycleStep:
        async def lead(_ctx: ExecutionContext, _term: Term) -> None:
            led.set()
            await asyncio.Event().wait()

        return leadership_lifecycle_step(
            LeadershipSpec(
                lock=LOCK,
                key="relay",
                campaign_every=timedelta(milliseconds=10),
            ),
            owner="replica-a",
            lead=lead,
        )

    async def test_a_tenant_aware_lock_is_refused_at_startup(self) -> None:
        led = asyncio.Event()
        step = self._step(led)

        runtime = _runtime(tenant_aware=True)

        async with runtime.scope():
            ctx = runtime.get_context()

            with pytest.raises(CoreException) as caught:
                await step.startup(ctx)

            assert not ctx.drainables.loops

        _refused(caught.value)
        assert not led.is_set()

    async def test_a_tenant_global_lock_leads(self) -> None:
        led = asyncio.Event()
        step = self._step(led)

        runtime = _runtime(tenant_aware=False)

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            try:
                async with asyncio.timeout(2):
                    await led.wait()

            finally:
                assert step.shutdown is not None
                await step.shutdown(ctx)
