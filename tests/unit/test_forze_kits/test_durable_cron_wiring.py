"""Cron-trigger auto-wiring: DurableFunctionCronTrigger specs become schedules.

# covers: DurableScheduler.ensure_schedule
# covers: DurableScheduler.ensure_cron_schedules
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze_kits.integrations.durable import (
    DurableScheduler,
    cron_schedule_id,
    durable_scheduler_background_lifecycle_step,
    resolve_durable_schedule_store,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #

UTC = UTC
_T0 = datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)


class _Args(BaseModel):
    pass


def _spec(name: str, *triggers: object) -> DurableFunctionSpec[_Args, _Args]:
    return DurableFunctionSpec(
        name=name,
        run=DurableFunctionInvokeSpec(args_type=_Args),
        triggers=tuple(triggers),  # type: ignore[arg-type]
    )


# ....................... #


class TestEnsureSchedule:
    async def test_creates_then_leaves_next_fire_on_reensure(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        scheduler = DurableScheduler()

        created = await scheduler.ensure_schedule(ctx, "s", "fn", "* * * * *", now=_T0)
        assert created.next_fire_at == datetime(2026, 1, 1, 0, 1, tzinfo=UTC)

        # Re-ensure later with the SAME cron must NOT reset next_fire (would skip a due fire).
        reensured = await scheduler.ensure_schedule(
            ctx, "s", "fn", "* * * * *", now=datetime(2026, 1, 1, 0, 0, 50, tzinfo=UTC)
        )
        assert reensured.next_fire_at == created.next_fire_at

        loaded = await resolve_durable_schedule_store(ctx).load("s")
        assert loaded is not None
        assert loaded.next_fire_at == created.next_fire_at

    async def test_re_ensure_for_an_explicit_tenant_finds_what_it_stored(self) -> None:
        """A schedule registered for a tenant nothing is bound to must still be found again.

        ``ensure_schedule`` reads before it writes, and the store scopes its key by the
        tenant. Naming a tenant on the write while reading unbound looks up a key the write
        never used, so every call re-puts and resets ``next_fire_at`` — a schedule that keeps
        skipping its own due fire, silently. The control-plane shape this protects: a caller
        that knows which tenant it is registering for and is bound to none of them.
        """

        ctx = context_from_modules(MockDepsModule())
        scheduler = DurableScheduler()
        tenant = UUID("00000000-0000-0000-0000-0000000000aa")

        created = await scheduler.ensure_schedule(
            ctx, "s", "fn", "* * * * *", tenant_id=tenant, now=_T0
        )
        reensured = await scheduler.ensure_schedule(
            ctx,
            "s",
            "fn",
            "* * * * *",
            tenant_id=tenant,
            # Past the first fire's minute, so a re-put moves the instant and the reset is
            # visible — inside the same minute both calls compute the same next fire and a
            # broken re-ensure looks identical to a working one.
            now=datetime(2026, 1, 1, 0, 1, 10, tzinfo=UTC),
        )

        assert reensured.next_fire_at == created.next_fire_at

    async def test_a_contradicted_tenant_is_still_refused(self) -> None:
        """Binding the explicit tenant must not launder a contradiction into a write.

        The store refuses a record naming a tenant the caller is not bound to. Binding the
        named tenant before the write would make that check compare the tenant against
        itself, so a caller bound to A could write B's schedule by asking for it — turning
        the refusal into the very cross-tenant write it exists to stop.
        """

        ctx = context_from_modules(MockDepsModule())
        scheduler = DurableScheduler()
        tenant_a = UUID("00000000-0000-0000-0000-0000000000aa")
        tenant_b = UUID("00000000-0000-0000-0000-0000000000bb")

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_a)):
            with pytest.raises(CoreException) as raised:
                await scheduler.ensure_schedule(
                    ctx, "s", "fn", "* * * * *", tenant_id=tenant_b, now=_T0
                )

        assert raised.value.code == "tenant_mismatch"
        assert await resolve_durable_schedule_store(ctx).load("s") is None

    async def test_a_contradicted_tenant_is_refused_on_the_no_op_path_too(self) -> None:
        """The early return is the half a create-path test cannot see.

        When the schedule already exists with the same cron, ``ensure_schedule`` returns it
        without ever reaching the store's write — so a contradiction checked only at the
        write is not checked at all here, and the caller is handed the *bound* tenant's
        schedule as the answer to a question about another one.
        """

        ctx = context_from_modules(MockDepsModule())
        scheduler = DurableScheduler()
        tenant_a = UUID("00000000-0000-0000-0000-0000000000aa")
        tenant_b = UUID("00000000-0000-0000-0000-0000000000bb")

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant_a)):
            await scheduler.ensure_schedule(ctx, "s", "fn", "* * * * *", now=_T0)

            with pytest.raises(CoreException) as raised:
                await scheduler.ensure_schedule(
                    ctx, "s", "fn", "* * * * *", tenant_id=tenant_b, now=_T0
                )

        assert raised.value.code == "tenant_mismatch"

    async def test_reregisters_when_cron_changes(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        scheduler = DurableScheduler()

        await scheduler.ensure_schedule(ctx, "s", "fn", "* * * * *", now=_T0)
        await scheduler.ensure_schedule(ctx, "s", "fn", "0 3 * * *", now=_T0)

        loaded = await resolve_durable_schedule_store(ctx).load("s")
        assert loaded is not None
        assert loaded.cron == "0 3 * * *"
        assert loaded.next_fire_at == datetime(2026, 1, 1, 3, tzinfo=UTC)


class TestEnsureCronSchedules:
    async def test_extracts_cron_triggers_and_skips_events(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        scheduler = DurableScheduler()
        store = resolve_durable_schedule_store(ctx)

        spec = _spec(
            "report",
            DurableFunctionCronTrigger(expression="0 3 * * *"),
            DurableFunctionEventTrigger(event="something-happened"),
        )

        ensured = await scheduler.ensure_cron_schedules(ctx, [spec], now=_T0)
        assert ensured == 1  # only the cron trigger

        # Load by the published helper, not a literal — guards the registrar and any control
        # plane against drifting off the `{name}:cron:{index}` convention.
        assert cron_schedule_id(spec, 0) == "report:cron:0"
        scheduled = await store.load(cron_schedule_id(spec, 0))
        assert scheduled is not None
        assert scheduled.name == "report"
        assert scheduled.cron == "0 3 * * *"

        # The event trigger (index 1) did not create a schedule.
        assert await store.load(cron_schedule_id(spec, 1)) is None

    async def test_lifecycle_step_auto_registers_at_startup(self) -> None:
        state = MockState()
        scheduler = DurableScheduler()
        spec = _spec("report", DurableFunctionCronTrigger(expression="0 3 * * *"))
        step = durable_scheduler_background_lifecycle_step(
            scheduler=scheduler,
            specs=[spec],
            interval=timedelta(hours=1),  # the fire loop won't tick during the test
        )
        runtime = ExecutionRuntime(
            deps=DepsRegistry.from_modules(MockDepsModule(state=state)).freeze()
        )

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            scheduled = await resolve_durable_schedule_store(ctx).load("report:cron:0")

            await step.shutdown(ctx)

        assert scheduled is not None
        assert scheduled.cron == "0 3 * * *"
