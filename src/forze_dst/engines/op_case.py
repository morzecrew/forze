"""OP_CASE engine — a seeded weighted workload of operation cases, run concurrently.

The simplest strategy: each seed draws its own workload of independent operation calls (picked by
weight, inputs auto-built) and runs them under controlled concurrency + scheduler perturbation. On
the first violating seed the workload is greedily minimized to a 1-minimal set that still fails and
returned as a reproducible report. Logic only — the run substrate lives in :mod:`forze_dst.engines.context`.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Sequence

from forze.base.primitives import derive_seed
from forze_dst.engines import base, context, projection
from forze_dst.engines.cases import OperationCase, Call
from forze_dst.oracle import ViolationReport
from forze_dst.oracle.recorder import History, Recorder
from forze_dst.runtime import run_simulation
from forze_dst.time_source import DEFAULT_EPOCH

if TYPE_CHECKING:
    from forze_dst.harness import Simulation

# ----------------------- #


def run_workload(
    sim: "Simulation",
    workload: Sequence[Call],
    *,
    concurrency: int,
    seed: int,
    schedule_seed: int | None,
    epoch: datetime,
    scheduler: object | None = None,
) -> History:
    """Run *workload* concurrently on the deterministic loop; return the recorded history."""

    recorder = Recorder(seed=seed)

    async def scenario() -> None:
        async with context.execution_context(sim, derive_seed(seed, "fault")) as ctx:
            if sim.setup is not None:
                await sim.setup(ctx)

            semaphore = asyncio.Semaphore(concurrency)
            await asyncio.gather(
                *(
                    context.run_call(
                        sim, ctx, semaphore, call_id=index, op=call.op, arg=call.arg
                    )
                    for index, call in enumerate(workload)
                )
            )

            if sim.observe is not None:
                await sim.observe(ctx)

            projection.fold_runtime_trace(ctx)

    context.run_recording(
        recorder,
        lambda: run_simulation(
            scenario,
            seed=derive_seed(seed, "entropy"),
            schedule_seed=schedule_seed,
            scheduler=scheduler,
            epoch=epoch,
            latency=context.latency_for(sim, seed),
        ),
    )

    return recorder.history


# ....................... #


def attempt(
    sim: "Simulation",
    *,
    cases: Sequence[OperationCase],
    count: int,
    concurrency: int,
    seed: int,
    perturb: bool,
    epoch: datetime,
    scheduler_factory: Callable[[int], object] | None = None,
) -> ViolationReport | None:
    """Run one seed's workload; on a violation, minimize and report (else ``None``)."""

    schedule_seed = derive_seed(seed, "schedule") if perturb else None
    workload = context.generate(sim, cases, count, seed)

    def run(items: Sequence[Call]) -> History:
        # A fresh scheduler per run (PCT is stateful) keeps the initial run, every minimization
        # predicate, and the final replay on the same interleaving.
        return run_workload(
            sim,
            items,
            concurrency=concurrency,
            seed=seed,
            schedule_seed=schedule_seed,
            epoch=epoch,
            scheduler=base.scheduler_for(seed, scheduler_factory),
        )

    return base.attempt_and_minimize(
        sim,
        seed=seed,
        schedule_seed=schedule_seed,
        run_initial=lambda: (run(workload), workload),
        run_subset=run,
        format_workload=lambda minimal: tuple(
            (call.op, call.arg) for call in minimal
        ),
    )


# ....................... #


def explore(
    sim: "Simulation",
    *,
    cases: Sequence[OperationCase],
    count: int = 50,
    concurrency: int = 4,
    seeds: Sequence[int],
    perturb: bool = True,
    epoch: datetime = DEFAULT_EPOCH,
    scheduler_factory: Callable[[int], object] | None = None,
) -> ViolationReport | None:
    """Generate + run a seeded workload per seed; on a violation, minimize and report.

    Each seed draws its own workload (operations + inputs) and, with *perturb*, its own
    interleaving. The first violating seed's workload is minimized to a 1-minimal set of
    operations that still fails; the report carries the seed, minimized workload, recorded
    history, and the registry fingerprint.
    """

    return base.explore_seeds(
        seeds,
        lambda seed: attempt(
            sim,
            cases=cases,
            count=count,
            concurrency=concurrency,
            seed=seed,
            perturb=perturb,
            epoch=epoch,
            scheduler_factory=scheduler_factory,
        ),
    )
