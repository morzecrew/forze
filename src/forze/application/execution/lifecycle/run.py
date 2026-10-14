"""Run resolved lifecycle graphs in wave order, and a scope's drain phases."""

import asyncio
from collections.abc import Coroutine, Sequence
from contextvars import Context
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final

from forze.application._logger import logger
from forze.application.contracts.execution import ExecutionGraph, LifecycleStep
from forze.base.primitives import StrKey

from ..graph_run import run_graph_waves_reverse

if TYPE_CHECKING:
    from ..context import DrainPhase, ExecutionContext

# ----------------------- #

DRAIN_PHASE_FLOOR_SECONDS: Final[float] = 0.1
"""A drain phase whose allowance falls below this is skipped rather than started: no
phase does useful work in less, and starting one only to cancel it at once is noise."""

DRAIN_PHASE_CANCEL_GRACE_SECONDS: Final[float] = 1.0
"""How long a drain phase cancelled at the end of its allowance gets to unwind before it is
abandoned. A drain therefore returns within its budget plus this."""

# ----------------------- #


class StartupWavePartialError(Exception):
    """Startup failed partway through a concurrent wave.

    Deliberately a raw ``Exception``, not ``CoreException``: a harness-level
    control signal consumed by the lifecycle runner itself (to roll back the
    completed steps before re-raising the original cause) — it must never be
    caught or mapped by the framework error envelope.
    """

    def __init__(self, cause: BaseException, completed: list[StrKey]) -> None:
        super().__init__(str(cause))

        self.cause = cause
        self.completed = completed


# ....................... #


async def _run_startup_step(
    step: LifecycleStep,
    ctx: "ExecutionContext",
) -> None:
    logger.trace("Executing '%s' startup hook", step.id)
    await step.startup(ctx)
    ctx.lifecycle_started.add(step.id)


# ....................... #


async def _run_shutdown_step(
    step: LifecycleStep,
    ctx: "ExecutionContext",
) -> None:
    logger.trace("Executing '%s' shutdown hook", step.id)
    await step.shutdown(ctx)


# ....................... #


async def _run_startup_wave_concurrent(
    graph: ExecutionGraph[LifecycleStep],
    ctx: "ExecutionContext",
    wave: tuple[StrKey, ...],
) -> list[StrKey]:
    if not wave:
        return []

    results = await asyncio.gather(
        *(_run_startup_step(graph.steps[step_id], ctx) for step_id in wave),
        return_exceptions=True,
    )
    completed: list[StrKey] = []
    first_error: BaseException | None = None

    for step_id, result in zip(wave, results, strict=True):
        if isinstance(result, BaseException):
            if first_error is None:
                first_error = result

        else:
            completed.append(step_id)

    if first_error is not None:
        raise StartupWavePartialError(first_error, completed) from first_error

    return completed


# ....................... #


async def _rollback_startup(
    graph: ExecutionGraph[LifecycleStep],
    ctx: "ExecutionContext",
    executed_waves: list[list[StrKey]],
) -> None:
    for wave_ids in reversed(executed_waves):
        for step_id in reversed(wave_ids):
            if step_id not in ctx.lifecycle_started:
                continue

            # Mark before attempting: shutdown runs at most once per startup,
            # even when it fails.
            ctx.lifecycle_started.discard(step_id)

            try:
                logger.trace("Rolling back '%s' via shutdown", step_id)
                await graph.steps[step_id].shutdown(ctx)
                logger.trace("Rolled back '%s' successfully", step_id)

            except Exception:
                logger.exception(
                    "Lifecycle rollback shutdown failed for '%s'",
                    step_id,
                )


# ....................... #


async def run_lifecycle_startup(
    graph: ExecutionGraph[LifecycleStep],
    ctx: "ExecutionContext",
    *,
    concurrent: bool,
) -> None:
    """Run startup hooks in forward wave order."""

    if graph.is_empty():
        return

    logger.trace(
        "Running lifecycle startup with %s step(s), concurrent=%s",
        len(graph.steps),
        concurrent,
    )

    executed_waves: list[list[StrKey]] = []

    if not concurrent:
        try:
            for wave in graph.waves:
                completed: list[StrKey] = []

                for step_id in wave:
                    await _run_startup_step(graph.steps[step_id], ctx)
                    completed.append(step_id)

                executed_waves.append(completed)

        except Exception:
            logger.exception("Lifecycle startup failed")
            await _rollback_startup(graph, ctx, executed_waves)
            raise

        return

    partial_error: StartupWavePartialError | None = None

    try:
        for wave in graph.waves:
            completed = await _run_startup_wave_concurrent(graph, ctx, wave)
            executed_waves.append(completed)

    except StartupWavePartialError as e:
        partial_error = e
        executed_waves.append(partial_error.completed)

    except Exception:
        logger.exception("Lifecycle startup failed")
        await _rollback_startup(graph, ctx, executed_waves)
        raise

    if partial_error is not None:
        logger.exception("Lifecycle startup failed")
        await _rollback_startup(graph, ctx, executed_waves)
        raise partial_error.cause from partial_error


# ....................... #


def _drop_pending_result(task: "asyncio.Task[None]") -> None:
    """Retrieve an abandoned shutdown task's eventual outcome.

    A hook abandoned on timeout may keep running (if it ignores cancellation); attaching
    this callback retrieves its result/exception when it finally settles, so asyncio does
    not warn about an unretrieved exception.
    """

    def _drain(finished: "asyncio.Future[None]") -> None:
        if not finished.cancelled():
            finished.exception()

    task.add_done_callback(_drain)


# ....................... #


async def _run_bounded(
    work: Coroutine[Any, Any, None],
    *,
    timeout: float,
    cancel_grace: float = 0.0,
    context: Context | None = None,
) -> tuple["asyncio.Task[None]", bool]:
    """Run *work* as a task for at most *timeout* seconds, then cancel and abandon it.

    Past *timeout* the task is cancelled and given up to *cancel_grace* seconds to unwind.
    One still running after that is abandoned: left to finish on its own, its outcome
    retrieved when it does. Not awaited, unlike ``asyncio.timeout`` — work that swallows the
    cancellation would hold the caller forever, and the callers here are on their way to
    process exit.

    :param context: Run *work* in a copy of this context rather than the caller's.
    :returns: The task, and whether it overran *timeout*. An overrun task that is not
        ``done()`` was abandoned.
    """

    task = (
        asyncio.ensure_future(work)
        if context is None
        else asyncio.get_running_loop().create_task(work, context=context.copy())
    )
    overran = False

    try:
        await asyncio.wait({task}, timeout=timeout)

        if not task.done():
            overran = True
            task.cancel()  # best-effort; the work may ignore it

            if cancel_grace > 0:
                await asyncio.wait({task}, timeout=cancel_grace)

    except asyncio.CancelledError:
        # Our wait was cancelled: don't leave the work running detached — cancel it,
        # drain its result so it can't warn on GC, then propagate the cancellation.
        task.cancel()
        _drop_pending_result(task)
        raise

    if overran:
        _drop_pending_result(task)

    return task, overran


# ....................... #


async def _run_shutdown_step_logged(
    step: LifecycleStep,
    ctx: "ExecutionContext",
    *,
    step_timeout: float | None = None,
) -> None:
    if step.id not in ctx.lifecycle_started:
        logger.trace(
            "Skipping '%s' shutdown hook (never started or already shut down)",
            step.id,
        )
        return

    # Mark before attempting: shutdown runs at most once per startup, even when
    # it fails.
    ctx.lifecycle_started.discard(step.id)

    if step_timeout is None:
        # Unbounded (direct callers / tests): swallow-and-log, never a timeout.
        try:
            await _run_shutdown_step(step, ctx)

        except Exception:
            logger.exception("Lifecycle shutdown failed for '%s'", step.id)

        return

    # Bounded teardown: abandon a hook that exceeds *step_timeout*, so a single wedged hook
    # (a broker flush that never returns, a connection that will not drain) can never block
    # teardown of the remaining steps — and thus process exit.
    task, overran = await _run_bounded(_run_shutdown_step(step, ctx), timeout=step_timeout)

    if overran:
        logger.error(
            "Lifecycle shutdown hook '%s' exceeded its %.1fs timeout; "
            "abandoning it and continuing teardown",
            step.id,
            step_timeout,
        )
        return

    error = task.exception()

    if error is not None:
        logger.error("Lifecycle shutdown failed for '%s'", step.id, exc_info=error)


# ....................... #


async def run_lifecycle_shutdown(
    graph: ExecutionGraph[LifecycleStep],
    ctx: "ExecutionContext",
    *,
    concurrent: bool,
    step_timeout: float | None = None,
) -> None:
    """Run shutdown hooks in reverse wave order.

    Each step is bounded by *step_timeout* seconds when set (``None`` leaves it
    unbounded); a hook that exceeds it is abandoned and logged so teardown of
    the remaining steps is never blocked by one wedged hook.
    """

    if graph.is_empty():
        return

    logger.trace(
        "Running lifecycle shutdown with %s step(s), concurrent=%s",
        len(graph.steps),
        concurrent,
    )

    if concurrent:

        async def _run_shutdown_concurrent_wave(wave: tuple[StrKey, ...]) -> None:
            if not wave:
                return

            # _run_shutdown_step_logged swallows (and logs) every Exception, so
            # gather results carry no step errors to inspect here.
            await asyncio.gather(
                *(
                    _run_shutdown_step_logged(graph.steps[step_id], ctx, step_timeout=step_timeout)
                    for step_id in wave
                ),
                return_exceptions=True,
            )

        for wave in reversed(graph.waves):
            await _run_shutdown_concurrent_wave(wave)

        return

    await run_graph_waves_reverse(
        graph,
        lambda step: _run_shutdown_step_logged(step, ctx, step_timeout=step_timeout),
        concurrent=False,
    )


# ....................... #


async def _run_drain_phase(phase: "DrainPhase", allowance: float) -> None:
    await phase.run(timedelta(seconds=allowance))


# ....................... #


async def run_drain_phases(
    phases: Sequence["DrainPhase"],
    *,
    budget: float,
    context: Context | None = None,
) -> None:
    """Run *phases* in order, all of them within *budget* seconds.

    Each phase runs in a copy of *context* when one is given — the scope's, so a phase never
    inherits the bindings of whichever task asked for the drain (a request's tenant, a
    deadline, an open transaction).

    Each phase may take ``min(max_duration, budget left)`` and is told that figure. A phase
    that overruns it is cancelled; one that ignores the cancellation is abandoned after
    :data:`DRAIN_PHASE_CANCEL_GRACE_SECONDS`. A phase left less than
    :data:`DRAIN_PHASE_FLOOR_SECONDS` is skipped. Every outcome other than success is
    logged, and a failed phase never stops the next one: this runs on the way to process
    exit, where nothing could act on an exception.
    """

    clock = asyncio.get_running_loop()
    deadline = clock.time() + budget

    for phase in phases:
        left = deadline - clock.time()
        allowance = (
            left if phase.max_duration is None else min(phase.max_duration.total_seconds(), left)
        )

        if allowance < DRAIN_PHASE_FLOOR_SECONDS:
            logger.warning(
                "Drain phase '%s' skipped: %.2fs of the drain budget left, under the %.1fs floor",
                phase.name,
                max(0.0, allowance),
                DRAIN_PHASE_FLOOR_SECONDS,
            )
            continue

        logger.trace("Running drain phase '%s' (%.2fs allowed)", phase.name, allowance)

        task, overran = await _run_bounded(
            _run_drain_phase(phase, allowance),
            timeout=allowance,
            cancel_grace=DRAIN_PHASE_CANCEL_GRACE_SECONDS,
            context=context,
        )

        if not task.done():
            logger.error(
                "Drain phase '%s' overran its %.2fs and ignored cancellation for %.1fs; "
                "abandoned it",
                phase.name,
                allowance,
                DRAIN_PHASE_CANCEL_GRACE_SECONDS,
            )

        elif overran:
            logger.warning(
                "Drain phase '%s' overran its %.2fs; cancelled it", phase.name, allowance
            )

        elif not task.cancelled() and (error := task.exception()) is not None:
            logger.error("Drain phase '%s' failed", phase.name, exc_info=error)
