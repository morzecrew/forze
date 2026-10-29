"""Scheduled purge of lapsed Postgres counters.

A counter written with a ``ttl`` reads as absent once it lapses, but Postgres keeps its row
until something deletes it. This step calls
:meth:`~forze_postgres.adapters.counter.PostgresCounterAdminAdapter.purge_expired` on an
interval, so a windowed counter (the login lockout, a daily quota) stays bounded without a
sweeper of your own.
"""

import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import cast, final
from uuid import UUID

import attrs

from forze.application.contracts.counter import CounterSpec
from forze.application.contracts.deps import backend_of
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.tenancy import TenantIdentity, require_tenant_global
from forze.application.contracts.tenancy.routed_client_base import RoutedTenantClientBase
from forze.application.execution import ExecutionContext
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.primitives import StrKey, current_entropy_source

from ...adapters.counter import PostgresCounterAdminAdapter
from ...kernel.relation import is_static_relation
from .._logger import logger
from .capabilities import POSTGRES_CLIENT_CAPABILITY

# ----------------------- #


def _tenant_global(purger: PostgresCounterAdminAdapter) -> bool:
    """Whether every tenant's purge of *purger*'s route would walk the same rows.

    A route is per tenant when it tags rows by tenant, resolves its table per tenant, or
    reaches a database per tenant; only a route that does none of these is global.
    """

    backend = cast(PostgresCounterAdminAdapter, backend_of(purger))

    return (
        not backend.tenant_aware
        and is_static_relation(backend.config.relation)
        and not isinstance(backend.client, RoutedTenantClientBase)
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _CounterPurgeStartup(LifecycleHook):
    """Periodically delete the lapsed rows of each counter route."""

    specs: Sequence[CounterSpec]
    interval: timedelta
    jitter: float
    tenants: Callable[[], Sequence[UUID]] | None

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(lambda: BackgroundLoopControl(name="postgres_counter_purge")),
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
        """Stop the loop between ticks. Idempotent — a purge deletes only lapsed rows, so a
        tick cut mid-purge costs nothing (the next one finishes it)."""

        return await self.control.stop(deadline=deadline)

    # ....................... #

    def _purger(self, ctx: ExecutionContext, spec: CounterSpec) -> PostgresCounterAdminAdapter:
        admin = ctx.counter.admin(spec)

        if not isinstance(backend_of(admin), PostgresCounterAdminAdapter):
            raise exc.configuration(
                f"Counter route {str(spec.name)!r} is not a Postgres counter, so the "
                "Postgres counter purge cannot purge it",
                details={"route": str(spec.name)},
            )

        # Called through the resolved port, so its tracing and policy wrappers still apply.
        return cast(PostgresCounterAdminAdapter, admin)

    # ....................... #

    async def _purge(self, ctx: ExecutionContext) -> None:
        for spec in self.specs:
            purged = await self._purger(ctx, spec).purge_expired()

            if purged:
                logger.info("Postgres counter purge", route=str(spec.name), purged=purged)

    # ....................... #

    async def _purge_tick(self, ctx: ExecutionContext, tenants: Sequence[UUID] | None) -> None:
        if tenants is None:
            await self._purge(ctx)
            return

        for tenant in tenants:
            try:
                with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                    await self._purge(ctx)

            except asyncio.CancelledError:
                raise

            except CoreException as error:
                # A wiring error (misrouted spec, missing dep) does not fix itself by
                # moving to the next tenant — terminate loudly. Operational failures
                # stay isolated per tenant.
                if error.kind is ExceptionKind.CONFIGURATION:
                    raise

                logger.exception("Postgres counter purge failed for tenant", tenant=str(tenant))

            except Exception:
                logger.exception("Postgres counter purge failed for tenant", tenant=str(tenant))

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Freeze the assigned tenant shard at startup (restart to repartition), matching
        # the sharded gateway/relay — a broken provider fails startup loudly.
        tenants = list(self.tenants()) if self.tenants is not None else None

        # A misrouted spec is refused here, not on the first tick of a loop nobody watches.
        purgers = {str(spec.name): self._purger(ctx, spec) for spec in self.specs}

        if tenants is None:
            # One unbound purge: tenant-scoped routes would refuse every one of them.
            for purger in purgers.values():
                require_tenant_global(purger, what="Postgres counter purge")

        else:
            for route, purger in purgers.items():
                if _tenant_global(purger):
                    raise exc.configuration(
                        f"Counter route {route!r} is tenant-global, so purging it once per "
                        "tenant would walk the same rows each time. Purge it in a step "
                        "without tenants=.",
                        details={"route": route},
                    )

            if not tenants:
                # More replicas than tenants leaves a shard empty, which is legitimate; a
                # provider returning nothing by mistake looks the same, so say it once.
                logger.warning(
                    "Postgres counter purge was assigned no tenants; it purges nothing "
                    "until restarted"
                )

        if self.control.running:
            logger.warning("Postgres counter purge already running; ignoring duplicate startup")
            return

        self.control.arm()

        async def _loop() -> None:
            while True:
                try:
                    await self._purge_tick(ctx, tenants)
                except asyncio.CancelledError:
                    raise
                except CoreException as error:
                    if error.kind is ExceptionKind.CONFIGURATION:
                        logger.exception(
                            "Postgres counter purge hit a configuration error; "
                            "loop stopped — fix the wiring and restart"
                        )
                        return

                    logger.exception("Postgres counter purge failed")
                except Exception:
                    logger.exception("Postgres counter purge failed")

                # Multiplicative jitter desynchronizes N replicas' purges; racing purges are
                # harmless (each deletes only lapsed rows), this only avoids redundant work.
                if await self.control.sleep_or_stop(
                    self.interval.total_seconds()
                    * (
                        1.0
                        + current_entropy_source().as_random().uniform(-self.jitter, self.jitter)
                    )
                ):
                    return

        self.control.task = asyncio.create_task(_loop(), name=self.control.loop_name)
        ctx.drainables.register(self)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _CounterPurgeShutdown(LifecycleHook):
    """Stop the purge loop (fallback for a hand-driven lifecycle; idempotent)."""

    startup: _CounterPurgeStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ....................... #


def postgres_counter_purge_lifecycle_step(
    *,
    specs: Sequence[CounterSpec],
    interval: timedelta = timedelta(minutes=10),
    jitter: float = 0.2,
    tenants: Callable[[], Sequence[UUID]] | None = None,
    step_id: StrKey = "postgres_counter_purge",
) -> LifecycleStep:
    """Periodically delete the lapsed rows of Postgres counter routes.

    Each tick calls
    :meth:`~forze_postgres.adapters.counter.PostgresCounterAdminAdapter.purge_expired` for
    every route in *specs*. A lapsed counter already reads as absent, so the purge reclaims
    storage and changes no answer. A table without the ``expires_at`` column holds nothing
    that can lapse, and its purge deletes nothing.

    A purge deletes only lapsed rows and re-checks each one, so any interval and any number of
    concurrent purges are safe: run the step unwrapped on every replica. A singleton step does
    not make it one purger: the guard holds its lock only while startup launches the loop, so
    later replicas purge too, and one that starts while another holds the lock never purges.
    Work that must run on one replica at a time belongs behind
    :func:`~forze_kits.lifecycle.leadership_lifecycle_step`.

    :param specs: Counter routes to purge; each must be wired to a Postgres counter
        (``PostgresDepsModule(counters=...)``), or startup fails with ``configuration``.
    :param interval: Time between purges.
    :param jitter: Fraction of *interval* each sleep varies by, in ``[0, 1)``.
    :param tenants: On per-tenant routes, the tenants to purge in turn, such as
        ``lambda: shard.tenants``, read once at startup. Startup fails over a tenant-global
        route, and an empty shard logs a warning and purges nothing. ``None`` purges the
        tenant-global rows once, and fails startup over a tenant-scoped route
        (``tenant_global_required``).
    :param step_id: Unique step id.
    :returns: A lifecycle step that needs a long-running process.
    :raises CoreException: ``configuration`` for no *specs*, a non-positive *interval* or a
        *jitter* outside ``[0, 1)``.
    """

    if not specs:
        raise exc.configuration("Postgres counter purge needs at least one counter spec")

    if interval.total_seconds() <= 0:
        raise exc.configuration("Counter purge interval must be positive")

    if not 0.0 <= jitter < 1.0:
        raise exc.configuration("Jitter must be in [0, 1)")

    startup = _CounterPurgeStartup(
        specs=tuple(specs),
        interval=interval,
        jitter=jitter,
        tenants=tenants,
    )

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_CounterPurgeShutdown(startup=startup),
        requires=(POSTGRES_CLIENT_CAPABILITY,),
        requires_long_running=True,
    )
