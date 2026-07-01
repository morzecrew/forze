"""Run resolved lifecycle graphs in wave order."""

import asyncio
from typing import TYPE_CHECKING

from forze.application._logger import logger
from forze.application.contracts.execution import ExecutionGraph, LifecycleStep
from forze.base.primitives import StrKey

from ..graph_run import run_graph_waves_reverse

if TYPE_CHECKING:
    from ..context import ExecutionContext

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

    # Bounded teardown: a hung shutdown hook is abandoned after *step_timeout* so
    # a single wedged hook (a broker flush that never returns, a connection that
    # will not drain) can never block teardown of the remaining steps — and thus
    # process exit — indefinitely. The drain window bounds in-flight work; this
    # bounds the teardown that follows it.
    try:
        async with asyncio.timeout(step_timeout):
            await _run_shutdown_step(step, ctx)

    except TimeoutError:
        logger.error(
            "Lifecycle shutdown hook '%s' exceeded its %.1fs timeout; "
            "abandoning it and continuing teardown",
            step.id,
            step_timeout,
        )

    except Exception:
        logger.exception("Lifecycle shutdown failed for '%s'", step.id)


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
                    _run_shutdown_step_logged(
                        graph.steps[step_id], ctx, step_timeout=step_timeout
                    )
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
