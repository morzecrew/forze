"""Run resolved lifecycle graphs in wave order."""

import asyncio
from typing import TYPE_CHECKING

from forze.application._logger import logger
from forze.application.contracts.execution import ExecutionGraph, LifecycleStep
from forze.base.primitives import StrKey

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #


class StartupWavePartialError(Exception):
    """Startup failed partway through a concurrent wave."""

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


# ....................... #


async def _run_shutdown_step(
    step: LifecycleStep,
    ctx: "ExecutionContext",
) -> None:
    logger.trace("Executing '%s' shutdown hook", step.id)
    await step.shutdown(ctx)


# ....................... #


async def _run_startup_wave_sequential(
    graph: ExecutionGraph[LifecycleStep],
    ctx: "ExecutionContext",
    wave: tuple[StrKey, ...],
) -> list[StrKey]:
    completed: list[StrKey] = []

    for step_id in wave:
        await _run_startup_step(graph.steps[step_id], ctx)
        completed.append(step_id)

    return completed


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
    run_wave = (
        _run_startup_wave_concurrent if concurrent else _run_startup_wave_sequential
    )
    partial_error: StartupWavePartialError | None = None

    try:
        for wave in graph.waves:
            completed = await run_wave(graph, ctx, wave)
            executed_waves.append(completed)

    except StartupWavePartialError as e:
        partial_error = e
        executed_waves.append(e.completed)

    except Exception:
        logger.exception("Lifecycle startup failed")
        await _rollback_startup(graph, ctx, executed_waves)
        raise

    if partial_error is not None:
        logger.exception("Lifecycle startup failed")
        await _rollback_startup(graph, ctx, executed_waves)
        raise partial_error.cause from partial_error


# ....................... #


async def _run_shutdown_wave_sequential(
    graph: ExecutionGraph[LifecycleStep],
    ctx: "ExecutionContext",
    wave: tuple[StrKey, ...],
) -> None:
    for step_id in reversed(wave):
        try:
            await _run_shutdown_step(graph.steps[step_id], ctx)

        except Exception:
            logger.exception("Lifecycle shutdown failed for '%s'", step_id)


# ....................... #


async def _run_shutdown_wave_concurrent(
    graph: ExecutionGraph[LifecycleStep],
    ctx: "ExecutionContext",
    wave: tuple[StrKey, ...],
) -> None:
    if not wave:
        return

    results = await asyncio.gather(
        *(_run_shutdown_step(graph.steps[step_id], ctx) for step_id in wave),
        return_exceptions=True,
    )

    for step_id, result in zip(wave, results, strict=True):
        if isinstance(result, Exception):
            logger.exception(
                "Lifecycle shutdown failed for '%s'",
                step_id,
                exc_info=result,
            )


# ....................... #


async def run_lifecycle_shutdown(
    graph: ExecutionGraph[LifecycleStep],
    ctx: "ExecutionContext",
    *,
    concurrent: bool,
) -> None:
    """Run shutdown hooks in reverse wave order."""

    if graph.is_empty():
        return

    logger.trace(
        "Running lifecycle shutdown with %s step(s), concurrent=%s",
        len(graph.steps),
        concurrent,
    )

    run_wave = (
        _run_shutdown_wave_concurrent if concurrent else _run_shutdown_wave_sequential
    )

    for wave in reversed(graph.waves):
        await run_wave(graph, ctx, wave)
