"""Durable background lifecycle hooks: config validation, drain, per-tenant, guards.

# covers: durable_recovery_background_lifecycle_step
# covers: durable_scheduler_background_lifecycle_step
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.base.exceptions import CoreException
from pydantic import BaseModel
from tests.support.execution_context import context_from_modules

from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableScheduler,
    durable_recovery_background_lifecycle_step,
    durable_scheduler_background_lifecycle_step,
)
from forze_kits.integrations.durable import lifecycle as lifecycle_mod
from forze_kits.integrations.durable.lifecycle import (
    _DurableRecoveryBackgroundShutdown,
    _DurableRecoveryBackgroundStartup,
    _DurableSchedulerBackgroundShutdown,
    _DurableSchedulerBackgroundStartup,
)
from forze_mock import MockDepsModule

# ----------------------- #


class _Args(BaseModel):
    pass


def _real_runner() -> DurableFunctionRunner:
    return DurableFunctionRunner(registry=DurableFunctionRegistry())


def _recovery_startup(**over: Any) -> _DurableRecoveryBackgroundStartup:
    kwargs: dict[str, Any] = {
        "runner": MagicMock(),
        "interval": timedelta(hours=1),
        "jitter": 0.0,
        "limit": 5,
        "max_batches_per_tick": 100,
        "max_concurrency": None,
        "tenants": None,
    }
    kwargs.update(over)
    return _DurableRecoveryBackgroundStartup(**kwargs)


def _scheduler_startup(**over: Any) -> _DurableSchedulerBackgroundStartup:
    kwargs: dict[str, Any] = {
        "scheduler": MagicMock(),
        "interval": timedelta(hours=1),
        "jitter": 0.0,
        "limit": 100,
        "max_batches_per_tick": 100,
        "tenants": None,
        "specs": (),
    }
    kwargs.update(over)
    return _DurableSchedulerBackgroundStartup(**kwargs)


def _cron_spec() -> DurableFunctionSpec[_Args, _Args]:
    return DurableFunctionSpec(
        name="report",
        run=DurableFunctionInvokeSpec(args_type=_Args),
        triggers=(DurableFunctionCronTrigger(expression="0 3 * * *"),),
    )


# ....................... #


class TestRecoveryConfigValidation:
    @pytest.mark.parametrize(
        "over,match",
        [
            ({"interval": timedelta(0)}, "Interval"),
            ({"jitter": 1.0}, "Jitter"),
            ({"jitter": -0.1}, "Jitter"),
            ({"limit": 0}, "Limit"),
            ({"max_batches_per_tick": 0}, "Max batches"),
        ],
    )
    def test_rejects_bad_config(self, over: dict[str, Any], match: str) -> None:
        with pytest.raises(CoreException, match=match):
            durable_recovery_background_lifecycle_step(runner=_real_runner(), **over)


class TestRecoveryDrain:
    async def test_drain_loops_until_a_short_batch(self) -> None:
        runner = MagicMock()
        runner.recover = AsyncMock(side_effect=[5, 5, 3])  # two full batches, then short
        startup = _recovery_startup(runner=runner, limit=5, max_batches_per_tick=10)

        await startup._drain(MagicMock())

        assert runner.recover.await_count == 3

    async def test_drain_stops_at_the_batch_cap(self) -> None:
        runner = MagicMock()
        runner.recover = AsyncMock(return_value=5)  # always a full batch
        startup = _recovery_startup(runner=runner, limit=5, max_batches_per_tick=2)

        await startup._drain(MagicMock())

        assert runner.recover.await_count == 2  # capped, never drains to empty


class TestRecoveryPerTenant:
    async def test_tick_binds_each_assigned_tenant(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        seen: list[Any] = []
        runner = MagicMock()

        async def _recover(c: Any, **_: Any) -> int:
            tenant = c.inv_ctx.get_tenant()
            seen.append(tenant.tenant_id if tenant else None)
            return 0

        runner.recover = AsyncMock(side_effect=_recover)
        a, b = uuid4(), uuid4()
        startup = _recovery_startup(runner=runner, tenants=lambda: [a, b])

        await startup._recover_tick(ctx, [a, b])

        assert seen == [a, b]

    async def test_tick_logs_and_continues_when_one_tenant_fails(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        runner = MagicMock()
        runner.recover = AsyncMock(side_effect=[RuntimeError("boom"), 0])
        a, b = uuid4(), uuid4()
        startup = _recovery_startup(runner=runner, tenants=lambda: [a, b])

        with patch.object(lifecycle_mod, "logger") as log:
            await startup._recover_tick(ctx, [a, b])  # must not raise

        assert runner.recover.await_count == 2  # tenant b still swept after a failed
        log.exception.assert_called_once()

    async def test_tick_propagates_cancellation(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        runner = MagicMock()
        runner.recover = AsyncMock(side_effect=asyncio.CancelledError())
        startup = _recovery_startup(runner=runner, tenants=lambda: [uuid4()])

        with pytest.raises(asyncio.CancelledError):
            await startup._recover_tick(ctx, [uuid4()])


class TestRecoveryTaskLifecycle:
    async def test_duplicate_startup_is_ignored(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        runner = MagicMock()
        runner.recover = AsyncMock(return_value=0)
        startup = _recovery_startup(runner=runner)
        shutdown = _DurableRecoveryBackgroundShutdown(startup=startup)

        await startup(ctx)
        first = startup.task

        with patch.object(lifecycle_mod, "logger") as log:
            await startup(ctx)  # task still running -> ignored

        assert startup.task is first
        log.warning.assert_called_once()

        await shutdown(ctx)

    async def test_shutdown_without_startup_is_a_noop(self) -> None:
        startup = _recovery_startup()
        shutdown = _DurableRecoveryBackgroundShutdown(startup=startup)

        await shutdown(MagicMock())  # task is None -> returns cleanly

    async def test_loop_logs_a_failing_sweep_and_keeps_running(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        runner = MagicMock()
        runner.recover = AsyncMock(side_effect=RuntimeError("boom"))
        startup = _recovery_startup(runner=runner, interval=timedelta(seconds=0.01))
        shutdown = _DurableRecoveryBackgroundShutdown(startup=startup)

        with patch.object(lifecycle_mod, "logger") as log:
            await startup(ctx)
            for _ in range(50):
                await asyncio.sleep(0.01)
                if log.exception.called:
                    break
            await shutdown(ctx)

        assert log.exception.called


# ....................... #


class TestSchedulerConfigValidation:
    @pytest.mark.parametrize(
        "over,match",
        [
            ({"interval": timedelta(0)}, "Interval"),
            ({"jitter": 1.0}, "Jitter"),
            ({"limit": 0}, "Limit"),
            ({"max_batches_per_tick": 0}, "Max batches"),
        ],
    )
    def test_rejects_bad_config(self, over: dict[str, Any], match: str) -> None:
        with pytest.raises(CoreException, match=match):
            durable_scheduler_background_lifecycle_step(
                scheduler=DurableScheduler(), **over
            )


class TestSchedulerDrain:
    async def test_drain_loops_until_a_short_batch(self) -> None:
        scheduler = MagicMock()
        scheduler.tick = AsyncMock(side_effect=[100, 100, 10])
        startup = _scheduler_startup(
            scheduler=scheduler, limit=100, max_batches_per_tick=10
        )

        await startup._drain(MagicMock())

        assert scheduler.tick.await_count == 3

    async def test_drain_stops_at_the_batch_cap(self) -> None:
        scheduler = MagicMock()
        scheduler.tick = AsyncMock(return_value=100)  # every batch full
        startup = _scheduler_startup(
            scheduler=scheduler, limit=100, max_batches_per_tick=2
        )

        await startup._drain(MagicMock())

        assert scheduler.tick.await_count == 2  # capped, never drains to empty


class TestSchedulerPerTenant:
    async def test_fire_tick_binds_each_assigned_tenant(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        seen: list[Any] = []
        scheduler = MagicMock()

        async def _tick(c: Any, **_: Any) -> int:
            tenant = c.inv_ctx.get_tenant()
            seen.append(tenant.tenant_id if tenant else None)
            return 0

        scheduler.tick = AsyncMock(side_effect=_tick)
        a, b = uuid4(), uuid4()
        startup = _scheduler_startup(scheduler=scheduler, tenants=lambda: [a, b])

        await startup._fire_tick(ctx, [a, b])

        assert seen == [a, b]

    async def test_fire_tick_logs_and_continues_when_one_tenant_fails(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        scheduler = MagicMock()
        scheduler.tick = AsyncMock(side_effect=[RuntimeError("boom"), 0])
        a, b = uuid4(), uuid4()
        startup = _scheduler_startup(scheduler=scheduler, tenants=lambda: [a, b])

        with patch.object(lifecycle_mod, "logger") as log:
            await startup._fire_tick(ctx, [a, b])

        assert scheduler.tick.await_count == 2
        log.exception.assert_called_once()

    async def test_ensure_cron_schedules_per_tenant_binds_each(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        seen: list[Any] = []
        scheduler = MagicMock()

        async def _ensure(c: Any, _specs: Any) -> int:
            tenant = c.inv_ctx.get_tenant()
            seen.append(tenant.tenant_id if tenant else None)
            return 1

        scheduler.ensure_cron_schedules = AsyncMock(side_effect=_ensure)
        a, b = uuid4(), uuid4()
        startup = _scheduler_startup(
            scheduler=scheduler, specs=[_cron_spec()], tenants=lambda: [a, b]
        )

        await startup._ensure_cron_schedules(ctx, [a, b])

        assert seen == [a, b]

    async def test_ensure_cron_schedules_without_specs_is_a_noop(self) -> None:
        scheduler = MagicMock()
        scheduler.ensure_cron_schedules = AsyncMock()
        startup = _scheduler_startup(scheduler=scheduler, specs=())

        await startup._ensure_cron_schedules(MagicMock(), None)

        scheduler.ensure_cron_schedules.assert_not_awaited()


class TestSchedulerTaskLifecycle:
    async def test_duplicate_startup_is_ignored(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        scheduler = MagicMock()
        scheduler.tick = AsyncMock(return_value=0)
        scheduler.ensure_cron_schedules = AsyncMock(return_value=0)
        startup = _scheduler_startup(scheduler=scheduler)
        shutdown = _DurableSchedulerBackgroundShutdown(startup=startup)

        await startup(ctx)
        first = startup.task

        with patch.object(lifecycle_mod, "logger") as log:
            await startup(ctx)

        assert startup.task is first
        log.warning.assert_called_once()

        await shutdown(ctx)

    async def test_shutdown_without_startup_is_a_noop(self) -> None:
        startup = _scheduler_startup()
        shutdown = _DurableSchedulerBackgroundShutdown(startup=startup)

        await shutdown(MagicMock())

    async def test_loop_logs_a_failing_sweep_and_keeps_running(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        scheduler = MagicMock()
        scheduler.tick = AsyncMock(side_effect=RuntimeError("boom"))
        scheduler.ensure_cron_schedules = AsyncMock(return_value=0)
        startup = _scheduler_startup(
            scheduler=scheduler, interval=timedelta(seconds=0.01)
        )
        shutdown = _DurableSchedulerBackgroundShutdown(startup=startup)

        with patch.object(lifecycle_mod, "logger") as log:
            await startup(ctx)
            for _ in range(50):
                await asyncio.sleep(0.01)
                if log.exception.called:
                    break
            await shutdown(ctx)

        assert log.exception.called
