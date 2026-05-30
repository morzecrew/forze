"""Run execution graph waves in forward or reverse order."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Mapping, TypeVar

from forze.application.contracts.execution import ExecutionGraph
from forze.base.primitives import StrKey

# ----------------------- #

G = TypeVar("G")


# ....................... #


async def run_wave_forward[G](
    wave: tuple[StrKey, ...],
    steps: Mapping[StrKey, G],
    run_step: Callable[[G], Awaitable[None]],
    *,
    concurrent: bool,
) -> None:
    """Run all steps in a single forward wave."""

    if not wave:
        return

    if concurrent:
        results = await asyncio.gather(
            *(run_step(steps[step_id]) for step_id in wave),
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, BaseException):
                raise result

        return

    for step_id in wave:
        await run_step(steps[step_id])


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
    steps: Mapping[StrKey, G],
    run_step: Callable[[G], Awaitable[None]],
    *,
    concurrent: bool,
) -> None:
    """Run all steps in a single reverse wave (last step id in wave first)."""

    step_ids = tuple(reversed(wave))

    if not step_ids:
        return

    if concurrent:
        results = await asyncio.gather(
            *(run_step(steps[step_id]) for step_id in step_ids),
            return_exceptions=True,
        )

        for result in results:
            if isinstance(result, BaseException):
                raise result

        return

    for step_id in step_ids:
        await run_step(steps[step_id])


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
