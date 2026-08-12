"""The Temporal worker as a lifecycle citizen.

"Workers are separate by design" was the right call about *deployment* and the wrong call
about *boilerplate*. Every adopter hand-writes the same loop: connect, build a
:class:`temporalio.worker.Worker`, handle signals, shut it down without cutting activities
off mid-flight — and gets the last part subtly wrong, because the interesting failure is a
worker that dies quietly at 3am and never comes back.

The framework already owns that shape for every other background plane, so the worker step
is composition, not new machinery: :func:`~forze.application.execution.background.run_supervised`
for jittered crash restart and the crash-loop ceiling,
:class:`~forze.application.execution.background.BackgroundLoopControl` for the stop signal
and bounded teardown, and ``ctx.drainables`` so the runtime brings the worker to rest
*before* lifecycle teardown — the same drain every consumer and relay gets.

What this step does not own: how many workers, which queues exist, deployment shape. It runs
**a** worker; topology is the operator's.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any, final

import attrs
from temporalio.worker import Worker

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.background import (
    DEFAULT_STOP_GRACE_SECONDS,
    BackgroundLoopControl,
    run_supervised,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ...kernel.client import TemporalClientPort
from ...sandbox import sandboxed_workflow_runner
from .._logger import logger

if TYPE_CHECKING:  # pragma: no cover
    from temporalio.client import Client
    from temporalio.worker import WorkflowRunner

    from forze.application.execution.context import ExecutionContext

# ----------------------- #

DEFAULT_WORKER_GRACEFUL_SHUTDOWN = timedelta(seconds=30)
"""How long in-flight activities get to finish once shutdown starts.

The SDK's own default is **zero** — activities are cancelled the moment the worker stops —
which turns every deploy into lost work that has to be retried. Thirty seconds is long
enough for an ordinary activity to land and short enough not to wedge a rollout; the
runtime's drain deadline caps it either way.
"""


@final
@attrs.define(slots=True, kw_only=True)
class _TemporalWorkerStartup(LifecycleHook):
    """Run a Temporal worker for the life of the process, supervised."""

    client: TemporalClientPort
    task_queue: str
    workflows: Sequence[type]
    activities: Sequence[Callable[..., Any]]
    name: str
    workflow_runner: WorkflowRunner
    graceful_shutdown: timedelta
    restart_backoff: timedelta
    max_concurrent_activities: int | None
    max_consecutive_crashes: int | None

    # ....................... #

    control: BackgroundLoopControl = attrs.field(
        default=attrs.Factory(
            lambda self: BackgroundLoopControl(
                name=self.name,
                # The loop is given at least the worker's own graceful window to come to
                # rest; a tighter runtime deadline still wins inside ``control.stop``.
                stop_grace=self.graceful_shutdown,
            ),
            takes_self=True,
        ),
        init=False,
    )
    """Stop signal and bounded teardown, shared with every other background loop."""

    worker: Worker | None = attrs.field(default=None, init=False, repr=False)
    """The worker of the *current* attempt; a restart builds a new one."""

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
        """Stop polling, let in-flight activities land, and return. Idempotent.

        Order matters. The stop signal is raised **before** the worker is asked to shut
        down: ``run_supervised`` treats a run that returns without a stop request as a
        fault and restarts it, so shutting the worker down first would race a fresh worker
        into existence during teardown.
        """

        self.control.request_stop()

        worker = self.worker

        if worker is not None and not worker.is_shutdown:
            await worker.shutdown()

        return await self.control.stop(deadline=deadline)

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Resolved here rather than inside the task: the client must already be connected,
        # and a lifecycle that ordered this step first should say so at boot instead of
        # crash-looping a worker that can never reach a server.
        native = self.client.native

        if self.control.running:
            logger.warning(
                "Temporal worker %s already running; ignoring duplicate startup",
                self.name,
            )
            return

        stop = self.control.arm()

        self.control.task = asyncio.create_task(
            run_supervised(
                lambda: self._run_once(native),
                stop=stop,
                name=self.control.loop_name,
                restart_backoff=self.restart_backoff,
                max_consecutive_crashes=self.max_consecutive_crashes,
            ),
            name=self.control.loop_name,
        )
        ctx.drainables.register(self)

    # ....................... #

    async def _run_once(self, native: Client) -> None:
        """One worker's lifetime: poll until shut down, or crash into the supervisor.

        A fresh :class:`Worker` per attempt, because a shut-down worker cannot be
        restarted — but the same *connection*, so a restart does not re-handshake.
        """

        if self.control.stopping:
            # Shutdown beat the loop's first tick (a failed boot, a health-check flap).
            # Without this, :meth:`stop` finds no worker to shut down, and the worker
            # built a moment later has nobody left to ask — it polls out the whole grace
            # window and is then cancelled mid-task.
            return

        worker = Worker(
            native,
            task_queue=self.task_queue,
            workflows=list(self.workflows),
            activities=list(self.activities),
            workflow_runner=self.workflow_runner,
            max_concurrent_activities=self.max_concurrent_activities,
            graceful_shutdown_timeout=self.graceful_shutdown,
        )
        self.worker = worker

        try:
            await worker.run()

        finally:
            self.worker = None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _TemporalWorkerShutdown(LifecycleHook):
    """Stop the worker.

    Normally a no-op — the runtime stops every registered loop before teardown begins. This
    is the fallback for a hand-driven lifecycle; ``stop`` is idempotent.
    """

    startup: _TemporalWorkerStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ----------------------- #


def temporal_worker_lifecycle_step(
    name: str = "temporal_worker",
    *,
    client: TemporalClientPort,
    task_queue: str,
    workflows: Sequence[type] = (),
    activities: Sequence[Callable[..., Any]] = (),
    max_concurrent_activities: int | None = None,
    workflow_runner: WorkflowRunner | None = None,
    graceful_shutdown: timedelta = DEFAULT_WORKER_GRACEFUL_SHUTDOWN,
    restart_backoff: timedelta = timedelta(seconds=5),
    max_consecutive_crashes: int | None = None,
    step_id: StrKey | None = None,
) -> LifecycleStep:
    """Run a Temporal worker under the runtime's supervision and drain.

    Order this **after** the step that connects *client*: the worker rides the framework
    client's connection through :attr:`~forze_temporal.TemporalClientPort.native`, so it
    inherits the configured data converter (payload encryption included) and every client
    interceptor that is also a worker interceptor — ``ExecutionContextInterceptor`` among
    them, which is how activities and workflows see identity, tenant and correlation
    without any wiring here. Starting this step first raises at boot rather than silently
    running a worker with a different (or no) codec.

    Workflow and activity *authoring* stays raw ``temporalio``: pass the classes and
    functions you already wrote. This step owns when they start and stop, nothing else.

    :param client: A connected framework client. A tenant-routed client is not meaningful
        here — a worker polls a queue, and has no request scope to resolve a tenant from.
    :param graceful_shutdown: How long in-flight activities get once shutdown starts. The
        runtime's drain deadline caps this; past it the loop is cancelled outright.
    :param restart_backoff: Base delay before a crashed worker is rebuilt, jittered.
    :param max_consecutive_crashes: Give up after this many short-lived runs in a row.
        ``None`` restarts forever, logging every crash loudly.
    """

    if graceful_shutdown.total_seconds() <= 0:
        # ``BackgroundLoopControl`` refuses a non-positive grace, and a zero window here
        # would mean cancelling activities mid-flight on every deploy.
        raise exc.configuration(
            f"Temporal worker {name!r} graceful_shutdown must be positive",
            code="core.temporal.worker_wiring",
        )

    if not workflows and not activities:
        # A worker with nothing registered polls a queue and answers nothing, so its tasks
        # time out and retry forever — a silent outage that looks like a healthy process.
        raise exc.configuration(
            f"Temporal worker {name!r} registers no workflows and no activities",
            code="core.temporal.worker_wiring",
        )

    startup = _TemporalWorkerStartup(
        client=client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
        name=name,
        workflow_runner=workflow_runner or sandboxed_workflow_runner(),
        graceful_shutdown=graceful_shutdown,
        restart_backoff=restart_backoff,
        max_concurrent_activities=max_concurrent_activities,
        max_consecutive_crashes=max_consecutive_crashes,
    )

    return LifecycleStep(
        id=step_id if step_id is not None else name,
        startup=startup,
        shutdown=_TemporalWorkerShutdown(startup=startup),
        requires_long_running=True,
    )
