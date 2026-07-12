"""Run execution graph waves in forward or reverse order."""

import asyncio
from typing import Awaitable, Callable

from forze.application.contracts.execution import ExecutionGraph
from forze.base.primitives import StrKey, StrKeyMapping

from .context.active_operation import continue_operation_on_task

# ----------------------- #


async def _run_wave[G](
    step_ids: tuple[StrKey, ...],
    steps: StrKeyMapping[G],
    run_step: Callable[[G], Awaitable[None]],
    *,
    concurrent: bool,
) -> None:
    """Run all steps in a single wave in ``step_ids`` order.

    When ``concurrent`` is true, all steps run together and every failure is
    collected: a single failing step re-raises its exception directly (so
    ``except SpecificError`` clauses keep working), while two or more failures
    raise an :class:`ExceptionGroup` (or :class:`BaseExceptionGroup` when a
    non-``Exception`` is among them) so no error is silently discarded.
    """

    if not step_ids:
        return

    if concurrent:
        # gather wraps each step coroutine in its own task. When a wave runs
        # inside an admitted operation, each step is an engine-internal
        # continuation of it (the gather is awaited right here): adopt the
        # operation onto the step task so a dispatch it makes rides the admitted
        # drain slot instead of being re-admitted. Outside an operation (e.g. a
        # lifecycle wave) the wrapper is a passthrough.
        results = await asyncio.gather(
            *(
                continue_operation_on_task(run_step(steps[step_id]))
                for step_id in step_ids
            ),
            return_exceptions=True,
        )

        failures = [r for r in results if isinstance(r, BaseException)]

        if len(failures) == 1:
            raise failures[0]

        if failures:
            # BaseExceptionGroup narrows to ExceptionGroup when all
            # failures are Exception instances.
            raise BaseExceptionGroup("graph wave step failures", failures)

        return

    for step_id in step_ids:
        await run_step(steps[step_id])


# ....................... #


async def run_wave_forward[G](
    wave: tuple[StrKey, ...],
    steps: StrKeyMapping[G],
    run_step: Callable[[G], Awaitable[None]],
    *,
    concurrent: bool,
) -> None:
    """Run all steps in a single forward wave.

    Multiple concurrent failures surface as an :class:`ExceptionGroup`; a
    single failure is re-raised directly (see :func:`_run_wave`).
    """

    await _run_wave(wave, steps, run_step, concurrent=concurrent)


# ....................... #


async def run_graph_waves_forward[G](
    graph: ExecutionGraph[G],
    run_step: Callable[[G], Awaitable[None]],
    *,
    concurrent: bool,
) -> None:
    """Run steps in forward topological wave order."""

    for wave in graph.waves:
        await run_wave_forward(wave, graph.steps, run_step, concurrent=concurrent)


# ....................... #


async def run_wave_reverse[G](
    wave: tuple[StrKey, ...],
    steps: StrKeyMapping[G],
    run_step: Callable[[G], Awaitable[None]],
    *,
    concurrent: bool,
) -> None:
    """Run all steps in a single reverse wave (last step id in wave first).

    Multiple concurrent failures surface as an :class:`ExceptionGroup`; a
    single failure is re-raised directly (see :func:`_run_wave`).
    """

    await _run_wave(tuple(reversed(wave)), steps, run_step, concurrent=concurrent)


# ....................... #


async def run_graph_waves_reverse[G](
    graph: ExecutionGraph[G],
    run_step: Callable[[G], Awaitable[None]],
    *,
    concurrent: bool,
) -> None:
    """Run steps in reverse topological wave order."""

    for wave in reversed(graph.waves):
        await run_wave_reverse(wave, graph.steps, run_step, concurrent=concurrent)
