"""Coverage sweep — the uniform seed sweep that stops once behaviour saturates.

The body of ``Simulation.coverage``: run the (auto-derived or given) scenario per seed, accumulate
behavioural coverage + reachability, stop early once coverage plateaus, and surface the first
violating seed's minimized report. Logic only — the caller manages the run-scoped ``active_config``.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import TYPE_CHECKING

from forze.base.primitives import derive_seed
from forze_dst.config import SimulationConfig
from forze_dst.oracle.confidence import ConfidenceProbe
from forze_dst.oracle.coverage import Behavior, CoverageStats, behavioral_coverage
from forze_dst.engines import scenario as scenario_engine
from forze_dst.oracle.invariants import check
from forze_dst.oracle import ViolationReport
from forze_dst.oracle.reachability import ReachabilityReport, reached_labels
from forze_dst.scenario import Scenario

if TYPE_CHECKING:
    from forze_dst.harness import Simulation

# ----------------------- #


def run_coverage(
    sim: "Simulation",
    config: SimulationConfig,
    *,
    scenario: Scenario | None = None,
) -> CoverageStats:
    """Coverage-guided sweep: explore seeds while behaviour grows, stop once it saturates."""

    sc = scenario if scenario is not None else sim.derive_scenario()

    # Honor the requested interleaving scheduler (PCT when selected), like run().
    factory = config.scheduler.factory()

    behaviors: set[Behavior] = set()
    new_by_seed: list[tuple[int, int]] = []
    seeds_run = 0
    plateau = 0
    plateaued = False
    violation: ViolationReport | None = None
    reach_hits: dict[str, int] = {t: 0 for t in config.reachability_targets}
    probe = ConfidenceProbe()

    for seed in config.seeds:
        schedule_seed = derive_seed(seed, "schedule") if config.perturb else None
        # Fresh PCT scheduler per seed (it is stateful).
        scheduler = None if factory is None else factory(derive_seed(seed, "schedule"))
        history, _ = scenario_engine.run_scenario(
            sim,
            sc,
            act_workload=None,
            act_count=config.act_count,
            concurrency=config.concurrency,
            seed=seed,
            schedule_seed=schedule_seed,
            epoch=config.epoch,
            scheduler=scheduler,
        )
        seeds_run += 1
        probe.observe(history)

        covered = behavioral_coverage(history)
        fresh = covered - behaviors
        new_by_seed.append((seed, len(fresh)))
        behaviors |= covered

        for label in reached_labels(history):
            reach_hits[label] = reach_hits.get(label, 0) + 1

        if check(history, sim.invariants):
            # A bug beats coverage: stop and hand back the minimized counterexample (the scenario
            # engine's attempt rebuilds a fresh scheduler per minimization run).
            violation = scenario_engine.attempt(
                sim,
                sc,
                act_count=config.act_count,
                concurrency=config.concurrency,
                seed=seed,
                perturb=config.perturb,
                epoch=config.epoch,
                scheduler_factory=factory,
            )
            if violation is not None:
                break

        if fresh:
            plateau = 0
        else:
            plateau += 1
            if config.coverage_plateau and plateau >= config.coverage_plateau:
                plateaued = True
                break

    reachability = (
        ReachabilityReport(
            targets=frozenset(config.reachability_targets),
            hits=MappingProxyType(reach_hits),
            runs=seeds_run,
        )
        if config.reachability_targets
        else None
    )

    return CoverageStats(
        behaviors=frozenset(behaviors),
        seeds_run=seeds_run,
        new_by_seed=tuple(new_by_seed),
        plateaued=plateaued,
        violation=violation,
        reachability=reachability,
        confidence=probe.report(faults=config.faults),
    )
