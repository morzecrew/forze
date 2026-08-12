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
from concurrent.futures import Executor
from contextlib import suppress
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

from ...kernel.client import RoutedTemporalClient, TemporalClientPort
from ...sandbox import sandboxed_workflow_runner
from .._logger import logger

if TYPE_CHECKING:  # pragma: no cover
    from temporalio.client import Client
    from temporalio.worker import WorkflowRunner

    from forze.application.execution.context import ExecutionContext

# ----------------------- #

DEFAULT_WORKER_GRACEFUL_SHUTDOWN = timedelta(seconds=10)
"""How long in-flight activities get to finish once shutdown starts.

The SDK's own default is **zero** — activities are cancelled the moment the worker stops —
which turns every deploy into lost work that has to be retried.

Ten seconds because that is :attr:`ExecutionRuntime.shutdown_step_timeout`'s default, and
the runtime hands *that* budget to every drainable loop at once. A window larger than it
cannot be honoured: the shared deadline fires mid-stop, the run is reported as a loop that
failed to come to rest, and — since one timeout cancels every loop's stop together — the
overrun is charged to the other loops too. Raise both together, never this one alone.
"""


def _log_shutdown_failure(task: asyncio.Task[None]) -> None:
    """Retrieve a shutdown task's outcome so a failure is logged, never stray."""

    if task.cancelled():
        return

    error = task.exception()

    if error is not None:
        logger.error("Temporal worker shutdown failed", exc_info=error)


# ....................... #


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
    activity_executor: Executor | None

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

        *deadline* is a hard bound, not a hint. The runtime stops every drainable loop
        against **one** shared deadline, and when it fires it cancels them all — so a
        worker that waited out its own graceful window regardless would spend other loops'
        shutdown budget, not just its own.
        """

        self.control.request_stop()

        worker = self.worker

        if worker is not None and not worker.is_shutdown:
            clock = asyncio.get_running_loop()
            budget = max(0.0, deadline - clock.time())

            # Its own task, because ``wait_for`` cancels what it waits on: the SDK is
            # already draining activities by then, and half-cancelling that is strictly
            # worse than letting it finish on its own while this call returns on time.
            # Overrunning leaves the task unawaited, so its outcome is claimed by the
            # callback rather than surfacing as a stray "exception was never retrieved".
            draining = asyncio.ensure_future(worker.shutdown())
            draining.add_done_callback(_log_shutdown_failure)

            # Neither a timeout nor a failed shutdown may skip what follows. Asking the
            # worker to stop is advisory; bringing the *loop* down is the outcome, and a
            # broken shutdown that took the stop sequence with it would leave the loop
            # running until something else cancelled it.
            with suppress(Exception):
                await asyncio.wait_for(asyncio.shield(draining), timeout=budget)

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

        try:
            worker = Worker(
                native,
                task_queue=self.task_queue,
                workflows=list(self.workflows),
                activities=list(self.activities),
                workflow_runner=self.workflow_runner,
                max_concurrent_activities=self.max_concurrent_activities,
                graceful_shutdown_timeout=self.graceful_shutdown,
                activity_executor=self.activity_executor,
            )

        except Exception as error:
            # Building a worker is pure validation — an undecorated workflow class, a
            # duplicate activity name, a client the bridge cannot use. None of it gets
            # better on the next attempt, so raising it as an ordinary crash would
            # rebuild-and-fail forever, one critical log per backoff. ``CONFIGURATION``
            # is the framework's marker for a fault retrying cannot clear, and
            # ``run_supervised`` stops on it.
            raise exc.configuration(
                f"Temporal worker {self.name!r} could not be built: {error}",
                code="core.temporal.worker_wiring",
            ) from error

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

        # At least the configured window: this path runs when nobody stopped the loop
        # first, and the shared five-second fallback would otherwise cut a deliberately
        # longer drain short — cancelling exactly the activities it was widened for.
        grace = max(
            DEFAULT_STOP_GRACE_SECONDS,
            self.startup.graceful_shutdown.total_seconds(),
        )

        await self.startup.stop(deadline=clock.time() + grace)


# ----------------------- #


def temporal_worker_lifecycle_step(
    name: str = "temporal_worker",
    *,
    client: TemporalClientPort,
    task_queue: str,
    workflows: Sequence[type] = (),
    activities: Sequence[Callable[..., Any]] = (),
    max_concurrent_activities: int | None = None,
    activity_executor: Executor | None = None,
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
    :param activity_executor: Required by the SDK as soon as any registered activity is a
        plain ``def`` rather than ``async def`` — without one, building the worker fails.
        A ``ThreadPoolExecutor`` is the usual choice; its size bounds how many synchronous
        activities run at once, so pair it with *max_concurrent_activities*.
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

    if isinstance(client, RoutedTemporalClient):
        # ``native`` on a routed client resolves *the calling scope's* tenant, and a
        # worker has no calling scope — it polls a queue. Left to startup this surfaces
        # as "Tenant ID is required", which reads like a missing binding rather than a
        # client that cannot back a worker at all.
        raise exc.configuration(
            f"Temporal worker {name!r} cannot run on a tenant-routed client: a worker "
            "polls a task queue and has no request scope to resolve a tenant from. Give "
            "it a TemporalClient, and run one worker process per tenant cluster.",
            code="core.temporal.worker_wiring",
        )

    if not workflows and not activities:
        # The SDK refuses this too, but only when the worker is *built* — which happens
        # inside the supervised loop, at startup, in a process that then reports a
        # configuration fault instead of never having started. Catching it at wiring time
        # puts the error where the mistake is.
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
        activity_executor=activity_executor,
    )

    return LifecycleStep(
        id=step_id if step_id is not None else name,
        startup=startup,
        shutdown=_TemporalWorkerShutdown(startup=startup),
        requires_long_running=True,
    )
