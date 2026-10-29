"""Durable background lifecycle hooks: config validation, drain, per-tenant, guards.

# covers: durable_recovery_background_lifecycle_step
# covers: durable_scheduler_background_lifecycle_step
# covers: durable_run_retention_lifecycle_step
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
    DurableRunStatus,
)
from forze.application.execution import DeploymentProfile
from forze.application.execution.lifecycle import LifecyclePlan
from forze.application.execution.runtime import ExecutionRuntime
from forze.application.integrations.crypto import PAYLOAD_CIPHER_MISSING_CODE
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_kits.integrations.durable import (
    DurableFunctionRegistry,
    DurableFunctionRunner,
    DurableScheduler,
    durable_recovery_background_lifecycle_step,
    durable_run_retention_lifecycle_step,
    durable_scheduler_background_lifecycle_step,
    resolve_durable_run_store,
)
from forze_kits.integrations.durable import lifecycle as lifecycle_mod
from forze_kits.integrations.durable.lifecycle import (
    _DurableRecoveryBackgroundShutdown,
    _DurableRecoveryBackgroundStartup,
    _DurableRetentionBackgroundStartup,
    _DurableSchedulerBackgroundShutdown,
    _DurableSchedulerBackgroundStartup,
)
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

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


class TestServerlessRefusal:
    # Both steps run a background loop for the process lifetime, which a function
    # frozen between invocations cannot host.

    @pytest.mark.parametrize(
        "step",
        [
            pytest.param(
                lambda: durable_recovery_background_lifecycle_step(runner=_real_runner()),
                id="recovery",
            ),
            pytest.param(
                lambda: durable_scheduler_background_lifecycle_step(scheduler=DurableScheduler()),
                id="scheduler",
            ),
            pytest.param(
                lambda: durable_run_retention_lifecycle_step(older_than=timedelta(days=7)),
                id="retention",
            ),
        ],
    )
    def test_serverless_runtime_refuses_the_step(self, step: Any) -> None:
        built = step()
        plan = LifecyclePlan.from_steps(built).freeze()

        with pytest.raises(CoreException) as ei:
            ExecutionRuntime(lifecycle=plan, deployment=DeploymentProfile.SERVERLESS)

        assert ei.value.kind is ExceptionKind.CONFIGURATION
        assert str(built.id) in ei.value.summary


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
            durable_scheduler_background_lifecycle_step(scheduler=DurableScheduler(), **over)


class TestSchedulerDrain:
    async def test_drain_loops_until_a_short_batch(self) -> None:
        scheduler = MagicMock()
        scheduler.tick = AsyncMock(side_effect=[100, 100, 10])
        startup = _scheduler_startup(scheduler=scheduler, limit=100, max_batches_per_tick=10)

        await startup._drain(MagicMock())

        assert scheduler.tick.await_count == 3

    async def test_drain_stops_at_the_batch_cap(self) -> None:
        scheduler = MagicMock()
        scheduler.tick = AsyncMock(return_value=100)  # every batch full
        startup = _scheduler_startup(scheduler=scheduler, limit=100, max_batches_per_tick=2)

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
        startup = _scheduler_startup(scheduler=scheduler, interval=timedelta(seconds=0.01))
        shutdown = _DurableSchedulerBackgroundShutdown(startup=startup)

        with patch.object(lifecycle_mod, "logger") as log:
            await startup(ctx)
            for _ in range(50):
                await asyncio.sleep(0.01)
                if log.exception.called:
                    break
            await shutdown(ctx)

        assert log.exception.called


# ....................... #


class TestRetention:
    @pytest.mark.parametrize(
        "over,match",
        [
            ({"interval": timedelta(0)}, "Interval"),
            ({"jitter": 1.0}, "Jitter"),
            ({"max_batches_per_tick": 0}, "Max batches"),
            ({"limit": 0}, "limit"),
            ({"older_than": timedelta(0)}, "older_than"),
            ({"statuses": [DurableRunStatus.RUNNING]}, "finished"),
            ({"statuses": [DurableRunStatus.FORWARD_INCOMPLETE]}, "finished"),
        ],
    )
    def test_rejects_bad_config(self, over: dict[str, Any], match: str) -> None:
        kwargs: dict[str, Any] = {"older_than": timedelta(days=7), **over}

        with pytest.raises(CoreException, match=match):
            durable_run_retention_lifecycle_step(**kwargs)

    async def test_drain_loops_until_a_short_batch_and_stops_at_the_cap(self) -> None:
        admin = MagicMock()
        admin.purge_finished = AsyncMock(side_effect=[5, 5, 3, 5, 5, 5])
        startup = _DurableRetentionBackgroundStartup(
            older_than=timedelta(days=1),
            statuses=(DurableRunStatus.COMPLETED,),
            interval=timedelta(hours=1),
            jitter=0.0,
            limit=5,
            max_batches_per_tick=2,
        )

        with patch.object(lifecycle_mod, "resolve_durable_run_admin", return_value=admin):
            await startup._drain(MagicMock())  # capped at two full batches
            await startup._drain(MagicMock())  # 3 < 5: short, stops after one

        assert admin.purge_finished.await_count == 3
        admin.purge_finished.assert_awaited_with(
            timedelta(days=1), statuses=(DurableRunStatus.COMPLETED,), limit=5
        )

    async def test_the_step_purges_finished_runs_and_keeps_live_ones(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        store = resolve_durable_run_store(ctx)
        done = await store.enqueue("done", input_json=None)
        await store.begin(done.run_id, lease_for=timedelta(minutes=5))
        await store.complete(done.run_id, output_json=None)
        live = await store.enqueue("live", input_json=None)

        step = durable_run_retention_lifecycle_step(
            older_than=timedelta(microseconds=1), interval=timedelta(hours=1)
        )

        await step.startup(ctx)
        for _ in range(50):
            if await store.load(done.run_id) is None:
                break
            await asyncio.sleep(0.01)
        await step.shutdown(ctx)

        assert await store.load(done.run_id) is None
        assert await store.load(live.run_id) is not None


    @pytest.mark.parametrize("shard", [None, [uuid4()]], ids=["unbound", "sharded"])
    async def test_startup_fails_without_the_admin_port(self, shard: list[Any] | None) -> None:
        step = durable_run_retention_lifecycle_step(
            older_than=timedelta(days=1), tenants=None if shard is None else lambda: shard
        )

        with pytest.raises(CoreException) as raised:
            await step.startup(context_from_modules())

        assert raised.value.kind is ExceptionKind.CONFIGURATION


# ....................... #


class TestConfigurationErrors:
    # Wiring does not fix itself, so a wiring error stops the loop once. A tenant's own
    # configuration error — its key disabled or pending deletion — is that tenant's: the
    # rest of the shard goes on, and the tenant is tried again next tick.

    @staticmethod
    def _startup(kind: str, work: AsyncMock, tenants: Any) -> Any:
        if kind == "recovery":
            runner = MagicMock()
            runner.recover = work
            return _recovery_startup(
                runner=runner, interval=timedelta(milliseconds=10), tenants=tenants
            )

        scheduler = MagicMock()
        scheduler.tick = work
        scheduler.ensure_cron_schedules = AsyncMock(return_value=0)
        return _scheduler_startup(
            scheduler=scheduler, interval=timedelta(milliseconds=10), tenants=tenants
        )

    @staticmethod
    async def _ended(startup: Any, *, settle: float) -> bool:
        ctx = context_from_modules(MockDepsModule())

        with patch.object(lifecycle_mod, "logger"):
            await startup(ctx)

            try:
                await asyncio.wait({startup.task}, timeout=settle)
                return startup.task.done()

            finally:
                await startup.stop(deadline=asyncio.get_running_loop().time() + 1)

    @pytest.mark.parametrize("shard", [None, [uuid4(), uuid4()]], ids=["unbound", "sharded"])
    @pytest.mark.parametrize("kind", ["recovery", "scheduler"])
    async def test_a_wiring_error_ends_the_loop(self, kind: str, shard: list[Any] | None) -> None:
        broken = AsyncMock(
            side_effect=exc.configuration("no keyring", code=PAYLOAD_CIPHER_MISSING_CODE)
        )
        startup = self._startup(kind, broken, (lambda: shard) if shard is not None else None)

        assert await self._ended(startup, settle=0.5)
        assert broken.await_count == 1

    @pytest.mark.parametrize("kind", ["recovery", "scheduler"])
    async def test_one_tenants_configuration_error_leaves_the_shard_running(
        self, kind: str
    ) -> None:
        a, b = uuid4(), uuid4()
        seen: list[Any] = []

        async def _work(ctx: Any, **_: Any) -> int:
            tenant = ctx.inv_ctx.get_tenant().tenant_id
            seen.append(tenant)

            if tenant == a:
                raise exc.configuration("AWS KMS key is disabled.")

            return 0

        startup = self._startup(kind, AsyncMock(side_effect=_work), lambda: [a, b])

        assert not await self._ended(startup, settle=0.3)
        assert seen.count(a) >= 2 and seen.count(b) >= 2

    @pytest.mark.parametrize("kind", ["recovery", "scheduler"])
    async def test_an_unbound_sweep_retries_a_configuration_error_that_is_not_wiring(
        self, kind: str
    ) -> None:
        work = AsyncMock(side_effect=exc.configuration("AWS KMS key is disabled."))
        startup = self._startup(kind, work, None)

        assert not await self._ended(startup, settle=0.3)
        assert work.await_count >= 2
