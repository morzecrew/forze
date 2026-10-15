"""Lifecycle steps run with no tenant bound, so they refuse a tenant-scoped port at startup.

A tenant-aware port refuses every call made outside a tenant with ``tenant_required``, an
authentication error no retry clears: a step over one failed at startup with an error that
named no wiring, or started and failed on every tick. Each step below now refuses it once, at
startup, as a configuration error.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
    DurableRunStoreDepKey,
    DurableScheduleStoreDepKey,
)
from forze.application.contracts.execution import LifecycleStep
from forze.application.contracts.tenancy import TenancyMixin
from forze.application.execution import (
    Deps,
    DepsRegistry,
    ExecutionContext,
    ExecutionRuntime,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_deps
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableScheduler,
    durable_recovery_background_lifecycle_step,
    durable_scheduler_background_lifecycle_step,
)
from forze_kits.integrations.outbox import outbox_relay_background_lifecycle_step
from forze_kits.integrations.progress import job_record_spec, job_staleness_lifecycle_step
from forze_kits.integrations.realtime import (
    realtime_cursor_spec,
    realtime_group_ensure_lifecycle_step,
    realtime_mailbox_retention_lifecycle_step,
    realtime_mailbox_spec,
    realtime_outbox_spec,
    realtime_relay_lifecycle_step,
    realtime_stream_spec,
    realtime_stream_trim_lifecycle_step,
)
from forze_kits.lifecycle import (
    LeadershipSpec,
    Term,
    leadership_lifecycle_step,
    singleton_lifecycle_step,
)
from forze_mock import MockDepsModule, MockRouteConfig

# ----------------------- #

LOCK = DistributedLockSpec(name="lifecycle-lock")


class _Args(BaseModel):
    pass


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


# ....................... #


def _routes_runtime(*names: str) -> ExecutionRuntime:
    routes = {name: MockRouteConfig(tenant_aware=True) for name in names}

    return ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule(routes=routes)).freeze())


async def _refused_at_startup(runtime: ExecutionRuntime, step: LifecycleStep) -> None:
    async with runtime.scope():
        ctx = runtime.get_context()

        try:
            with pytest.raises(CoreException) as caught:
                await step.startup(ctx)

        finally:
            if step.shutdown is not None:
                await step.shutdown(ctx)

        assert not ctx.drainables.loops

    _refused(caught.value)


class TestRealtimeSteps:
    async def test_group_ensure_refuses_a_tenant_aware_stream(self) -> None:
        step = realtime_group_ensure_lifecycle_step(stream_spec=realtime_stream_spec())

        await _refused_at_startup(_routes_runtime("realtime"), step)

    async def test_stream_trim_without_tenants_refuses_a_tenant_aware_stream(self) -> None:
        step = realtime_stream_trim_lifecycle_step(stream_spec=realtime_stream_spec())

        await _refused_at_startup(_routes_runtime("realtime"), step)

    async def test_stream_trim_with_tenants_binds_them(self) -> None:
        step = realtime_stream_trim_lifecycle_step(
            stream_spec=realtime_stream_spec(), tenants=lambda: [uuid4()]
        )
        runtime = _routes_runtime("realtime")

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)
            assert step.shutdown is not None
            await step.shutdown(ctx)

    async def test_mailbox_retention_without_tenants_refuses_a_tenant_aware_mailbox(
        self,
    ) -> None:
        step = realtime_mailbox_retention_lifecycle_step(max_age=timedelta(days=1))

        await _refused_at_startup(_routes_runtime(str(realtime_mailbox_spec().name)), step)

    async def test_mailbox_retention_without_tenants_refuses_tenant_aware_cursors(
        self,
    ) -> None:
        step = realtime_mailbox_retention_lifecycle_step(max_age=timedelta(days=1))

        await _refused_at_startup(_routes_runtime(str(realtime_cursor_spec().name)), step)

    @pytest.mark.parametrize("relay", ["outbox", "realtime"])
    async def test_a_relay_without_tenants_refuses_a_tenant_aware_outbox(self, relay: str) -> None:
        step = (
            outbox_relay_background_lifecycle_step(
                outbox_spec=realtime_outbox_spec(),
                transport="stream",
                stream_spec=realtime_stream_spec(),
            )
            if relay == "outbox"
            else realtime_relay_lifecycle_step(
                outbox_spec=realtime_outbox_spec(), stream_spec=realtime_stream_spec()
            )
        )

        await _refused_at_startup(_routes_runtime("realtime"), step)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _TenantAwareStore(TenancyMixin):
    """A durable store wired tenant-aware: the adapters carry the flag the same way."""

    tenant_aware: bool = True


def _durable_context() -> ExecutionContext:
    store = _TenantAwareStore()

    return context_from_deps(
        Deps.plain(
            {
                DurableRunStoreDepKey: lambda _ctx: store,
                DurableScheduleStoreDepKey: lambda _ctx: store,
            }
        )
    )


class TestDurableSteps:
    @pytest.mark.parametrize(
        "step",
        [
            pytest.param(
                durable_recovery_background_lifecycle_step(
                    runner=DurableFunctionRunner(registry=DurableFunctionRegistry())
                ),
                id="recovery",
            ),
            pytest.param(
                durable_scheduler_background_lifecycle_step(scheduler=DurableScheduler()),
                id="scheduler",
            ),
            pytest.param(
                durable_scheduler_background_lifecycle_step(
                    scheduler=DurableScheduler(),
                    specs=[
                        DurableFunctionSpec(
                            name="report",
                            run=DurableFunctionInvokeSpec(args_type=_Args),
                            triggers=(DurableFunctionCronTrigger(expression="0 3 * * *"),),
                        )
                    ],
                ),
                id="scheduler-with-cron-specs",
            ),
        ],
    )
    async def test_a_step_without_tenants_refuses_a_tenant_aware_store(
        self, step: LifecycleStep
    ) -> None:
        ctx = _durable_context()

        try:
            with pytest.raises(CoreException) as caught:
                await step.startup(ctx)

        finally:
            if step.shutdown is not None:
                await step.shutdown(ctx)

        _refused(caught.value)
        assert not ctx.drainables.loops


    async def test_the_scheduler_holds_only_its_schedule_store_to_the_rule(self) -> None:
        # The scheduler fires each schedule under that schedule's tenant, so the run store
        # it enqueues into is never called unbound and may be tenant-aware.
        @attrs.define(slots=True, kw_only=True, frozen=True)
        class _SharedSchedules:
            async def claim_due(self, **_: object) -> list[object]:
                return []

        ctx = context_from_deps(
            Deps.plain(
                {
                    DurableRunStoreDepKey: lambda _ctx: _TenantAwareStore(),
                    DurableScheduleStoreDepKey: lambda _ctx: _SharedSchedules(),
                }
            )
        )
        step = durable_scheduler_background_lifecycle_step(scheduler=DurableScheduler())

        await step.startup(ctx)

        assert step.shutdown is not None
        await step.shutdown(ctx)


# ....................... #


class TestJobStaleness:
    async def test_without_tenants_it_refuses_a_tenant_aware_collection(self) -> None:
        step, _monitor = job_staleness_lifecycle_step(silent_after=timedelta(minutes=5))

        await _refused_at_startup(_routes_runtime(str(job_record_spec().name)), step)
