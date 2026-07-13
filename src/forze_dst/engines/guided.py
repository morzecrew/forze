"""Coverage-guided mutation engine wiring — drive the AFL-style fuzzer over a Simulation.

Bridges the substrate-free mutation engine (:mod:`forze_dst.explore_guided`) to a real Simulation:
builds the genome→history runner (workload from op indices, run on the op-case substrate), the
seed genome (a normal op-case workload at the master seed), and the on-violation minimizer, then
runs :func:`~forze_dst.explore_guided.coverage_guided_search`. Logic only — the caller manages the
run-scoped ``active_config``.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from typing import TYPE_CHECKING

from forze.base.primitives import derive_seed
from forze_dst.config import SimulationConfig
from forze_dst.engines import base, context, op_case
from forze_dst.engines.cases import Call, OperationCase
from forze_dst.explore_guided import Genome, GuidedStats, coverage_guided_search
from forze_dst.oracle import ViolationReport
from forze_dst.oracle.invariants import check
from forze_dst.oracle.recorder import History
from forze_dst.scheduler import Reorderer

if TYPE_CHECKING:
    from forze_dst.harness import Simulation

# ----------------------- #


def run_guided(
    sim: Simulation,
    config: SimulationConfig,
    *,
    cases: Sequence[OperationCase],
) -> GuidedStats:
    """Run a coverage-guided mutation sweep over *cases* (the body of ``Simulation.coverage_guided``)."""

    catalog = list(cases)
    if not catalog:
        raise ValueError("coverage_guided requires cases=")

    master = next(iter(config.seeds), 0)
    max_ops = max(config.count * 2, config.count + 8)

    factory = config.scheduler.factory()

    def make_scheduler(seed: int) -> Reorderer | None:
        # Fresh per run (a PCT scheduler is stateful), seeded from the genome's seed.
        return None if factory is None else factory(derive_seed(seed, "schedule"))

    def workload_of(genome: Genome) -> list[Call]:
        rng = random.Random(  # nosec B311 - seeded input generation, not crypto
            derive_seed(genome.seed, "input")
        )
        return [
            Call(
                op=catalog[index].op,
                arg=context.input_for(sim, catalog[index].op, rng, catalog[index]),
            )
            for index in genome.ops
        ]

    def run_items(items: Sequence[Call], seed: int) -> History:
        return op_case.run_workload(
            sim,
            items,
            concurrency=config.concurrency,
            seed=seed,
            schedule_seed=(derive_seed(seed, "schedule") if config.perturb else None),
            epoch=config.epoch,
            scheduler=make_scheduler(seed),
        )

    def run_genome(genome: Genome) -> History:
        return run_items(workload_of(genome), genome.seed)

    def on_violation(genome: Genome) -> ViolationReport | None:
        # The skeleton's initial check doubles as the heisenbug guard: a genome that fails during
        # search but not on a clean re-run yields no violation → reports nothing.
        workload = workload_of(genome)

        return base.attempt_and_minimize(
            sim,
            seed=genome.seed,
            schedule_seed=(derive_seed(genome.seed, "schedule") if config.perturb else None),
            run_initial=lambda: (run_items(workload, genome.seed), workload),
            run_subset=lambda subset: run_items(subset, genome.seed),
            format_workload=lambda minimal: tuple((call.op, call.arg) for call in minimal),
        )

    # The initial workload mirrors a normal op-case run at the master seed.
    gen_rng = random.Random(  # nosec B311 - seeded workload generation, not crypto
        derive_seed(master, "input")
    )
    seed_genome = Genome(
        ops=tuple(
            gen_rng.choices(
                range(len(catalog)),
                weights=[case.weight for case in catalog],
                k=config.count,
            )
        ),
        seed=master,
    )

    return coverage_guided_search(
        seed_genome=seed_genome,
        run=run_genome,
        is_violation=lambda history: bool(check(history, sim.invariants)),
        on_violation=on_violation,
        master_seed=master,
        budget=config.guided_budget,
        catalog_size=len(catalog),
        max_ops=max_ops,
    )
