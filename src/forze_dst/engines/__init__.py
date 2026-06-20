"""Exploration engines — one module per strategy, behind the thin ``Simulation`` facade.

Each engine is a set of free functions taking the :class:`~forze_dst.harness.Simulation` as its
run context (its registry, deps factory, invariants, hooks): :mod:`op_case`, :mod:`scenario`
(scenario + Hypothesis + DPOR), :mod:`crash_restart`, :mod:`guided`, plus the :mod:`coverage_sweep`
and :mod:`derive_probe` helpers. :func:`dispatch` is the strategy router the facade's ``run`` calls;
the others are exposed for direct use and for the documented plugin seam.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from forze_dst.engines.cases import OperationCase
from forze_dst.config import SchedulerKind, SimulationConfig, Strategy
from forze_dst.engines import (
    coverage_sweep,
    crash_restart,
    derive_probe,
    guided,
    op_case,
    scenario as scenario_engine,
)
from forze_dst.oracle import ViolationReport
from forze_dst.scenario import Scenario
from forze_dst.scheduler import pct_scheduler_factory

if TYPE_CHECKING:
    from forze_dst.harness import Simulation

# ----------------------- #

run_coverage = coverage_sweep.run_coverage
run_guided = guided.run_guided
reactive_map = derive_probe.reactive_map
derive_scenario = derive_probe.derive_scenario

__all__ = [
    "dispatch",
    "op_case",
    "scenario_engine",
    "crash_restart",
    "guided",
    "coverage_sweep",
    "derive_probe",
    "run_coverage",
    "run_guided",
    "reactive_map",
    "derive_scenario",
]


# ....................... #


def _pct(config: SimulationConfig):  # type: ignore[no-untyped-def]
    """A PCT scheduler factory when PCT is selected, else ``None`` (the default shuffle)."""

    return (
        pct_scheduler_factory(depth=config.pct_depth, steps=config.pct_steps)
        if config.scheduler is SchedulerKind.PCT
        else None
    )


def dispatch(
    sim: "Simulation",
    config: SimulationConfig,
    *,
    scenario: Scenario | None = None,
    cases: Sequence[OperationCase] | None = None,
) -> ViolationReport | None:
    """Route a ``run`` to the engine ``config.strategy`` (and ``config.crash``) selects.

    The body of ``Simulation.run``: a crash policy turns the run into the crash/restart scenario;
    OP_CASE needs *cases*; the scenario strategies use *scenario* (auto-derived when omitted).
    Returns the first violating seed's minimized, reproducible counterexample, or ``None``.
    """

    if config.crash is not None:
        # Crash → restart → recovery: scenario-shaped (arrange/act). Honors the chosen interleaving
        # scheduler (PCT when selected) just like the scenario strategy.
        sc = scenario if scenario is not None else sim.derive_scenario()
        return crash_restart.explore(
            sim,
            sc,
            act_count=config.act_count,
            concurrency=config.concurrency,
            seeds=config.seeds,
            perturb=config.perturb,
            epoch=config.epoch,
            scheduler_factory=_pct(config),
        )

    if config.strategy is Strategy.OP_CASE:
        if cases is None:
            raise ValueError("OP_CASE strategy requires cases=")

        return op_case.explore(
            sim,
            cases=cases,
            count=config.count,
            concurrency=config.concurrency,
            seeds=config.seeds,
            perturb=config.perturb,
            epoch=config.epoch,
            scheduler_factory=_pct(config),
        )

    sc = scenario if scenario is not None else sim.derive_scenario()

    if config.strategy is Strategy.SCENARIO:
        return scenario_engine.explore(
            sim,
            sc,
            act_count=config.act_count,
            concurrency=config.concurrency,
            seeds=config.seeds,
            perturb=config.perturb,
            epoch=config.epoch,
            scheduler_factory=_pct(config),
        )

    if config.strategy is Strategy.HYPOTHESIS:
        return scenario_engine.explore_hypothesis(
            sim,
            sc,
            max_act=config.act_count,
            concurrency=config.concurrency,
            perturb=config.perturb,
            epoch=config.epoch,
            max_examples=config.max_examples,
            scheduler_factory=_pct(config),
        )

    # DPOR — drives its own systematic scheduler over one fixed workload.
    return scenario_engine.explore_dpor(
        sim,
        sc,
        act_count=config.act_count,
        concurrency=config.concurrency,
        seed=config.dpor_seed,
        max_runs=config.max_runs,
        epoch=config.epoch,
    )
