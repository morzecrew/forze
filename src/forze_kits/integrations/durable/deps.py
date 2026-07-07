"""Container wiring for the self-hosted durable orchestrators (runner + scheduler).

Unlike the durable **stores** (execution-scoped ``SimpleDepPort`` keys registered by a
backend's deps module), the runner and scheduler are app-built singletons: the runner closes
over an application-specific :class:`DurableFunctionRegistry`, so the framework cannot
construct it. This module registers a pair the application already built under well-known
keys — mirroring :class:`~forze_kits.adapters.secrets.deps.SecretsDepsModule` — so request
handlers reach them with :func:`resolve_durable_runner` / :func:`resolve_durable_scheduler`
instead of inventing string keys.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.deps import DepKey, Deps, DepsModule

from .registry import DurableFunctionRegistry
from .runner import DurableFunctionRunner
from .scheduler import DurableScheduler
from .telemetry import DurableTelemetry

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

DurableRunnerDepKey = DepKey[DurableFunctionRunner]("durable_function_runner")
"""Key the app-built :class:`DurableFunctionRunner` singleton is registered under."""

DurableSchedulerDepKey = DepKey[DurableScheduler]("durable_function_scheduler")
"""Key the app-built :class:`DurableScheduler` singleton is registered under."""


# ....................... #


def resolve_durable_runner(ctx: ExecutionContext) -> DurableFunctionRunner:
    """Resolve the durable-function runner registered in *ctx*.

    Use it from a request handler to ``enqueue`` / ``run_now`` a run without holding a
    reference to the runner the wiring built.
    """

    return ctx.deps.provide(DurableRunnerDepKey)


# ....................... #


def resolve_durable_scheduler(ctx: ExecutionContext) -> DurableScheduler:
    """Resolve the durable scheduler registered in *ctx*.

    Use it from a control-plane handler to ``put`` / ``remove`` a schedule (the scheduler
    validates the cron expression and computes the first fire, so prefer it over writing the
    schedule store directly).
    """

    return ctx.deps.provide(DurableSchedulerDepKey)


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DurableKitsDepsModule(DepsModule):
    """Register a pre-built durable runner + scheduler under their well-known keys."""

    runner: DurableFunctionRunner
    """The runner whose registry drives / recovers runs (also passed to the recovery step)."""

    scheduler: DurableScheduler
    """The scheduler that fires cron schedules (also passed to the scheduler step)."""

    # ....................... #

    def __call__(self) -> Deps:
        return Deps.plain(
            {
                DurableRunnerDepKey: self.runner,
                DurableSchedulerDepKey: self.scheduler,
            }
        )


# ....................... #


def durable_kits_deps(
    *,
    registry: DurableFunctionRegistry,
    lease_for: timedelta = timedelta(minutes=5),
    heartbeat_divisor: int = 3,
    telemetry: DurableTelemetry | None = None,
) -> tuple[Deps, DurableFunctionRunner, DurableScheduler]:
    """Build the durable runner + scheduler from *registry* and register both in one call.

    Returns the :class:`Deps` to merge into the container **and** the two instances, so the
    same runner/scheduler can be handed to
    :func:`~forze_kits.integrations.durable.durable_recovery_background_lifecycle_step` and
    :func:`~forze_kits.integrations.durable.durable_scheduler_background_lifecycle_step`. The
    registry stays application-owned — the framework only wraps what the app built.
    """

    runner = DurableFunctionRunner(
        registry=registry,
        lease_for=lease_for,
        heartbeat_divisor=heartbeat_divisor,
        telemetry=telemetry,
    )
    scheduler = DurableScheduler(telemetry=telemetry)

    deps = DurableKitsDepsModule(runner=runner, scheduler=scheduler)()

    return deps, runner, scheduler
