"""Seed-driven workload generation for simulation.

Turn a catalog of named operations into a deterministic, weighted-random workload and
run it under controlled concurrency — so one seed produces a reproducible fuzz of an
app's operations, and (with scheduler perturbation) explores the interleavings between
them. Vary the seed to explore the space; a fixed seed replays exactly, so any failure
it surfaces reproduces.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime
from typing import Awaitable, Callable, Sequence, final

import attrs

from .runtime import run_simulation
from .time_source import DEFAULT_EPOCH

# ----------------------- #

__all__ = [
    "OpSpec",
    "generate_workload",
    "run_workload",
    "simulate_workload",
]



@final
@attrs.define(frozen=True, kw_only=True)
class OpSpec:
    """One operation in a workload catalog: a name, a coroutine factory, and a weight."""

    name: str
    """Logical operation name (for traces / debugging)."""

    make: Callable[[], Awaitable[object]]
    """Produces a *fresh* coroutine each call — invoked once per occurrence."""

    weight: float = 1.0
    """Relative selection weight when generating a workload."""


# ....................... #


def generate_workload(
    catalog: Sequence[OpSpec],
    *,
    seed: int,
    count: int,
) -> list[OpSpec]:
    """Pick *count* ops from *catalog* by weight, deterministically from *seed*."""

    if not catalog:
        raise ValueError("workload catalog is empty")

    if count < 0:
        raise ValueError("count must be non-negative")

    rng = random.Random(seed)  # nosec B311 - deterministic workload generation, not crypto
    weights = [op.weight for op in catalog]

    if any(weight < 0.0 for weight in weights):
        raise ValueError("OpSpec weights must be non-negative")

    if sum(weights) <= 0.0:
        raise ValueError("at least one OpSpec must have a positive weight")

    return rng.choices(list(catalog), weights=weights, k=count)


# ....................... #


async def run_workload(
    workload: Sequence[OpSpec],
    *,
    concurrency: int = 1,
) -> list[object]:
    """Run *workload* ops as tasks, at most *concurrency* in flight; results in order.

    Concurrency is what creates the interleavings scheduler perturbation then explores —
    a non-atomic operation racing with its peers surfaces under some schedule.
    """

    if concurrency < 1:
        raise ValueError("concurrency must be >= 1")

    semaphore = asyncio.Semaphore(concurrency)
    results: list[object] = [None] * len(workload)

    async def _run(index: int, op: OpSpec) -> None:
        async with semaphore:
            results[index] = await op.make()

    await asyncio.gather(*(_run(i, op) for i, op in enumerate(workload)))

    return results


# ....................... #


def simulate_workload(
    catalog: Sequence[OpSpec],
    *,
    seed: int = 0,
    count: int = 50,
    concurrency: int = 4,
    perturb: bool = True,
    epoch: datetime = DEFAULT_EPOCH,
) -> list[object]:
    """Generate a seeded workload from *catalog* and run it under the simulation loop.

    With *perturb* (default), scheduler perturbation is on (its seed derived from
    *seed*) so the run explores concurrent interleavings; turn it off for plain FIFO.
    Returns the per-op results in workload order. Vary *seed* to explore; a fixed seed
    reproduces exactly.
    """

    workload = generate_workload(catalog, seed=seed, count=count)

    async def _scenario() -> list[object]:
        return await run_workload(workload, concurrency=concurrency)

    return run_simulation(
        _scenario,
        seed=seed,
        epoch=epoch,
        schedule_seed=seed if perturb else None,
    )
