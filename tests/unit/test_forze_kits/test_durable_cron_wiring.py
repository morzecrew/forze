"""Cron-trigger auto-wiring: DurableFunctionCronTrigger specs become schedules.

# covers: DurableScheduler.ensure_schedule
# covers: DurableScheduler.ensure_cron_schedules
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.application.execution import DepsRegistry, ExecutionRuntime
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.durable import (
    DurableScheduler,
    durable_scheduler_background_lifecycle_step,
    resolve_durable_schedule_store,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #

UTC = timezone.utc
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

        scheduled = await store.load("report:cron:0")
        assert scheduled is not None
        assert scheduled.name == "report"
        assert scheduled.cron == "0 3 * * *"

        # The event trigger (index 1) did not create a schedule.
        assert await store.load("report:cron:1") is None

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
