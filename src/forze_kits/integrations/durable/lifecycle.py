"""Lifecycle helper: a background scanner that recovers abandoned durable runs."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import Any, final
from uuid import UUID

import attrs

from forze.application.contracts.durable.function import DurableFunctionSpec
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import StrKey, current_entropy_source
from forze_kits.integrations._logger import logger
from forze_kits.lifecycle import DEFAULT_STOP_GRACE_SECONDS, BackgroundLoopControl

from .runner import DurableFunctionRunner
from .scheduler import DurableScheduler

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _DurableRecoveryBackgroundStartup(LifecycleHook):
    """Start a background task that periodically recovers abandoned durable runs."""

    runner: DurableFunctionRunner
    """The runner whose registry re-invokes each reclaimed run."""

    interval: timedelta
    """Delay between recovery sweeps."""

    jitter: float
    """Multiplicative sleep jitter in ``[0, 1)`` (desynchronizes N replicas' scanners)."""

    limit: int
    """Runs claimed per sweep batch (also the drain-detection threshold)."""

    max_batches_per_tick: int
    """Safety cap on batches per sweep so a large backlog cannot starve the loop."""

    max_concurrency: int | None
    """Recover a batch's runs concurrently up to this bound (``None`` = sequential)."""

    tenants: Callable[[], Sequence[UUID]] | None = None
    """When set, recover **per tenant** (namespace tier): each sweep binds every assigned
    tenant in turn and recovers its per-tenant table. The shard is frozen at startup. ``None``
    recovers unbound — one pass over a tagged table claims every tenant's runs."""

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(name="durable_recovery"),
            takes_self=True,
        ),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    # ....................... #

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The running loop, if any."""

        return self.control.task

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop the loop at its next tick boundary. Idempotent.

        A sweep cut mid-claim leaves a run leased to a process that is going away; it is
        recovered by the next one, but only after the lease expires. Stopping between ticks
        costs nothing and hands the work over cleanly.
        """

        return await self.control.stop(deadline=deadline)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.interval.total_seconds() <= 0:
            raise exc.configuration("Interval must be positive")

        if not 0.0 <= self.jitter < 1.0:
            raise exc.configuration("Jitter must be in [0, 1)")

        if self.limit < 1:
            raise exc.configuration("Limit must be >= 1")

        if self.max_batches_per_tick < 1:
            raise exc.configuration("Max batches per tick must be >= 1")

    # ....................... #

    async def _drain(self, ctx: ExecutionContext) -> None:
        """Drain abandoned runs (under whatever tenant is bound): recover until short."""

        for _ in range(self.max_batches_per_tick):
            claimed = await self.runner.recover(
                ctx, limit=self.limit, max_concurrency=self.max_concurrency
            )

            if claimed < self.limit:
                break

    # ....................... #

    async def _recover_tick(
        self,
        ctx: ExecutionContext,
        tenants: Sequence[UUID] | None,
    ) -> None:
        if tenants is None:
            await self._drain(ctx)
            return

        for tenant in tenants:
            try:
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    await self._drain(ctx)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Durable recovery failed for tenant", tenant=str(tenant))

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Freeze the assigned tenant shard at startup (restart to repartition), matching the
        # outbox relay — a broken provider fails startup loudly instead of a silent dead task.
        tenants = list(self.tenants()) if self.tenants is not None else None

        async def _loop() -> None:
            while True:
                try:
                    await self._recover_tick(ctx, tenants)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Durable recovery sweep failed")

                # Multiplicative jitter desynchronizes N replicas' scanners so they don't
                # synchronize into a thundering herd against the claim query.
                if await self.control.sleep_or_stop(
                    self.interval.total_seconds()
                    * (
                        1.0
                        + current_entropy_source().as_random().uniform(-self.jitter, self.jitter)
                    )
                ):
                    return

        if self.control.running:
            logger.warning("Durable recovery already running; ignoring duplicate startup")
            return

        self.control.arm()
        self.control.task = asyncio.create_task(_loop(), name=self.control.loop_name)
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _DurableRecoveryBackgroundShutdown(LifecycleHook):
    """Stop the background durable-recovery loop.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This is
    the fallback for a hand-driven lifecycle; :meth:`stop` is idempotent.
    """

    startup: _DurableRecoveryBackgroundStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ....................... #


def durable_recovery_background_lifecycle_step(
    *,
    runner: DurableFunctionRunner,
    interval: timedelta = timedelta(seconds=30),
    jitter: float = 0.2,
    limit: int = 10,
    max_batches_per_tick: int = 100,
    max_concurrency: int | None = None,
    tenants: Callable[[], Sequence[UUID]] | None = None,
    step_id: StrKey = "durable_recovery",
) -> LifecycleStep:
    """Build a lifecycle step that recovers abandoned durable runs on a background interval.

    Each sweep **drains the backlog**: batches of up to *limit* abandoned runs (a due
    ``PENDING`` run or a ``RUNNING`` run past its lease) are re-claimed and re-invoked until
    a sweep returns fewer than *limit*, capped at *max_batches_per_tick*; then the task
    sleeps *interval* with multiplicative *jitter*. A run's completed steps replay from the
    journal rather than re-running (exactly-once for the recorded result; a body may re-run if
    a worker is reclaimed / crashes before it journals, so keep step bodies idempotent).
    *max_concurrency* bounds how many runs a batch recovers at once (``None`` = sequential).

    When *tenants* is set the store is **namespace-tier** (per-tenant tables): each sweep
    binds every assigned tenant in turn and recovers its table (shard frozen at startup;
    assign the shard this instance owns and shard across instances to parallelize). Omit it
    for a tagged (shared-table) store — one unbound sweep recovers every tenant's runs and
    the runner re-binds each run's tenant to execute it.

    Concurrent scanners are safe (``FOR UPDATE SKIP LOCKED`` + a fence on the terminal
    write), so this can run on every replica; pair with the ``forze_kits`` singleton
    lifecycle guard if you prefer a single elected scanner. Production deployments often
    prefer an external cron / workflow scheduler over in-process polling.
    """

    startup = _DurableRecoveryBackgroundStartup(
        runner=runner,
        interval=interval,
        jitter=jitter,
        limit=limit,
        max_batches_per_tick=max_batches_per_tick,
        max_concurrency=max_concurrency,
        tenants=tenants,
    )
    shutdown = _DurableRecoveryBackgroundShutdown(startup=startup)

    return LifecycleStep(id=step_id, startup=startup, shutdown=shutdown)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _DurableSchedulerBackgroundStartup(LifecycleHook):
    """Start a background task that fires due recurring schedules."""

    scheduler: DurableScheduler
    interval: timedelta
    jitter: float
    limit: int
    max_batches_per_tick: int
    tenants: Callable[[], Sequence[UUID]] | None = None
    specs: Sequence[DurableFunctionSpec[Any, Any]] = ()
    """Durable-function specs whose cron triggers are auto-registered as schedules at
    startup (idempotent — a restart re-uses an unchanged schedule, so no due fire is lost)."""

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(name="durable_scheduler"),
            takes_self=True,
        ),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    # ....................... #

    @property
    def task(self) -> asyncio.Task[None] | None:
        """The running loop, if any."""

        return self.control.task

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return self.control.loop_name

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop the cron-driven loop at its next tick boundary. Idempotent.

        The scheduler only *fires* what is due: it claims nothing and holds no lease, so a tick
        cut short costs no more than the schedules it had not reached — and those are still due
        on the next process's first tick. Stopping between ticks just avoids interrupting a fire
        already under way.
        """

        return await self.control.stop(deadline=deadline)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.interval.total_seconds() <= 0:
            raise exc.configuration("Interval must be positive")

        if not 0.0 <= self.jitter < 1.0:
            raise exc.configuration("Jitter must be in [0, 1)")

        if self.limit < 1:
            raise exc.configuration("Limit must be >= 1")

        if self.max_batches_per_tick < 1:
            raise exc.configuration("Max batches per tick must be >= 1")

    # ....................... #

    async def _drain(self, ctx: ExecutionContext) -> None:
        """Fire due schedules (under whatever tenant is bound) until a sweep comes short."""

        for _ in range(self.max_batches_per_tick):
            fired = await self.scheduler.tick(ctx, limit=self.limit)

            if fired < self.limit:
                break

    # ....................... #

    async def _fire_tick(
        self,
        ctx: ExecutionContext,
        tenants: Sequence[UUID] | None,
    ) -> None:
        if tenants is None:
            await self._drain(ctx)
            return

        for tenant in tenants:
            try:
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    await self._drain(ctx)

            except asyncio.CancelledError:
                raise

            except Exception:
                logger.exception("Durable scheduler failed for tenant", tenant=str(tenant))

    # ....................... #

    async def _ensure_cron_schedules(
        self,
        ctx: ExecutionContext,
        tenants: Sequence[UUID] | None,
    ) -> None:
        if not self.specs:
            return

        if tenants is None:
            await self.scheduler.ensure_cron_schedules(ctx, self.specs)
            return

        for tenant in tenants:
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                await self.scheduler.ensure_cron_schedules(ctx, self.specs)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        tenants = list(self.tenants()) if self.tenants is not None else None

        # Auto-register schedules from declared cron triggers before the fire loop starts.
        await self._ensure_cron_schedules(ctx, tenants)

        async def _loop() -> None:
            while True:
                try:
                    await self._fire_tick(ctx, tenants)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Durable scheduler sweep failed")

                if await self.control.sleep_or_stop(
                    self.interval.total_seconds()
                    * (
                        1.0
                        + current_entropy_source().as_random().uniform(-self.jitter, self.jitter)
                    )
                ):
                    return

        if self.control.running:
            logger.warning("Durable scheduler already running; ignoring duplicate startup")
            return

        self.control.arm()
        self.control.task = asyncio.create_task(_loop(), name=self.control.loop_name)
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _DurableSchedulerBackgroundShutdown(LifecycleHook):
    """Stop the background durable-scheduler loop.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This is
    the fallback for a hand-driven lifecycle; :meth:`stop` is idempotent.
    """

    startup: _DurableSchedulerBackgroundStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ....................... #


def durable_scheduler_background_lifecycle_step(
    *,
    scheduler: DurableScheduler,
    interval: timedelta = timedelta(seconds=30),
    jitter: float = 0.2,
    limit: int = 100,
    max_batches_per_tick: int = 100,
    tenants: Callable[[], Sequence[UUID]] | None = None,
    specs: Sequence[DurableFunctionSpec[Any, Any]] = (),
    step_id: StrKey = "durable_scheduler",
) -> LifecycleStep:
    """Build a lifecycle step that fires due recurring schedules on a background interval.

    Each sweep claims due schedules, enqueues one run per schedule, and advances each to its
    next occurrence (fire-once / skip-missed), draining until a sweep is short of *limit*,
    then sleeps *interval* with multiplicative *jitter*. The enqueued runs are executed by
    the recovery scanner / runner — run this alongside
    :func:`durable_recovery_background_lifecycle_step`.

    *specs* auto-registers a schedule for every ``DurableFunctionCronTrigger`` declared on a
    durable-function spec (idempotent — a restart re-uses an unchanged schedule, so no due
    fire is lost), so cron triggers "just schedule" without a manual ``scheduler.put``.

    When *tenants* is set the schedule store is **namespace-tier**: each sweep binds every
    assigned tenant in turn and fires its schedules (and the specs are ensured per tenant);
    the shard is frozen at startup. Omit it for a tagged store (one unbound sweep).

    Concurrent schedulers are safe (idempotent run keys + compare-and-set advance), so this
    can run on every replica; pair with the singleton lifecycle guard for a single elected
    scheduler. Production deployments often prefer an external scheduler over in-process
    polling.
    """

    startup = _DurableSchedulerBackgroundStartup(
        scheduler=scheduler,
        interval=interval,
        jitter=jitter,
        limit=limit,
        max_batches_per_tick=max_batches_per_tick,
        tenants=tenants,
        specs=specs,
    )
    shutdown = _DurableSchedulerBackgroundShutdown(startup=startup)

    return LifecycleStep(id=step_id, startup=startup, shutdown=shutdown)
