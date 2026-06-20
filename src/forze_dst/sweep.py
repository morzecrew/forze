"""Parallel timelines — fan a seed sweep across processes, aggregate coverage + violations.

DST's whole premise is that one seed reproduces one run *in a single process* (the loop, the
seam ContextVars, the entropy/clock binding are all per-process). That makes **inter-seed**
parallelism trivially safe — distinct seeds share nothing — even though loop-internal threading is
forbidden. This module spreads disjoint seeds across a process pool so a nightly fuzz explores
thousands of timelines per wall-hour, then folds every worker's result into one
:class:`SweepResult`: which seeds violated, the union of behaviours covered, and a throughput
metric (simulated virtual time per wall second — how much system time the fleet simulated, and how
fast).

The engine is substrate-free: it runs any picklable ``run(seed) -> SeedOutcome``. The bundled
:class:`SimulationSeedRunner` is the batteries-included one — it resolves a ``module:attr`` import
string to a :class:`~forze_dst.harness.Simulation` inside each worker (the object itself never
crosses the process boundary, only the string), so it is picklable where a closure would not be.
"""

from __future__ import annotations

import importlib
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Callable, Sequence, final

import attrs

from forze_dst.config import SchedulerKind, SimulationConfig
from forze_dst.coverage import Behavior
from forze_dst.faults import FaultPolicy, FaultRule
from forze_dst.latency import Constant, LatencyProfile, LatencyRule

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class SeedOutcome:
    """One seed's result — picklable, so it crosses back from a worker process intact."""

    seed: int
    violated: bool
    """Whether this seed tripped an invariant (re-run it for the full minimized report)."""
    behaviors: frozenset[Behavior]
    """The PII-free behaviours this run exercised (see :func:`~forze_dst.behavioral_coverage`)."""
    sim_seconds: float = 0.0
    """Virtual time the run spanned, when the runner can supply it — fuels the time-dilation
    metric. ``0.0`` when not available (the sweep then reports throughput as runs/second)."""


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class SweepResult:
    """Every worker's outcomes folded into one picture: coverage, violations, throughput."""

    runs: int
    violations: tuple[int, ...]
    """Seeds that tripped an invariant, ascending."""
    behaviors: frozenset[Behavior]
    """Union of behaviours covered across the whole sweep."""
    simulated_seconds: float
    """Total virtual time simulated across all seeds (``0.0`` when no runner supplied it)."""
    wall_seconds: float
    """Wall-clock time the sweep took (measured, not simulated)."""

    # ....................... #

    @property
    def first_violation(self) -> int | None:
        """The lowest violating seed — the one to re-run for a minimized report."""

        return self.violations[0] if self.violations else None

    # ....................... #

    @property
    def runs_per_second(self) -> float:
        """Sweep throughput in seeds explored per wall second."""

        return self.runs / self.wall_seconds if self.wall_seconds > 0 else 0.0

    # ....................... #

    @property
    def time_dilation(self) -> float:
        """Simulated virtual seconds per wall second — how much system time the fleet simulated
        per real second. ``0.0`` when no runner supplied ``sim_seconds``."""

        return (
            self.simulated_seconds / self.wall_seconds if self.wall_seconds > 0 else 0.0
        )

    # ....................... #

    def format(self) -> str:
        """Render a short human summary of the sweep."""

        lines = [
            "DST parallel sweep",
            f"  seeds run:      {self.runs}",
            f"  behaviors:      {len(self.behaviors)}",
            f"  throughput:     {self.runs_per_second:.1f} seeds/s",
        ]

        if self.simulated_seconds > 0:
            lines.append(
                f"  time dilation:  {self.time_dilation:.0f}× (sim s / wall s)"
            )

        if self.violations:
            lines.append(
                f"  ✗ violations:   {len(self.violations)} (first at seed {self.first_violation})"
            )

        return "\n".join(lines)


# ....................... #


def _aggregate(outcomes: Sequence[SeedOutcome], *, wall_seconds: float) -> SweepResult:
    behaviors: set[Behavior] = set()
    violations: list[int] = []
    simulated = 0.0

    for outcome in outcomes:
        behaviors |= outcome.behaviors
        simulated += outcome.sim_seconds
        if outcome.violated:
            violations.append(outcome.seed)

    return SweepResult(
        runs=len(outcomes),
        violations=tuple(sorted(violations)),
        behaviors=frozenset(behaviors),
        simulated_seconds=simulated,
        wall_seconds=wall_seconds,
    )


def sweep(
    run: Callable[[int], SeedOutcome],
    seeds: Sequence[int],
) -> SweepResult:
    """Run every seed sequentially and fold the outcomes — the single-process baseline.

    Same aggregation (and same per-seed results) as :func:`parallel_sweep`, so it is the reference
    a parallel run must match, and the fallback where a process pool is unwanted.
    """

    start = time.perf_counter()
    outcomes = [run(seed) for seed in seeds]
    return _aggregate(outcomes, wall_seconds=time.perf_counter() - start)


def parallel_sweep(
    run: Callable[[int], SeedOutcome],
    seeds: Sequence[int],
    *,
    workers: int | None = None,
    chunk: int = 1,
) -> SweepResult:
    """Distribute *seeds* across a process pool, run each in its own process, fold the results.

    *run* must be **picklable** (a top-level function or a frozen instance like
    :class:`SimulationSeedRunner`, not a closure) because it crosses the process boundary. Each
    seed is fully deterministic in its worker — the seam ContextVars bind per process — so the
    folded :class:`SweepResult` is identical to :func:`sweep`'s; only wall time (and so throughput)
    differs. *workers* defaults to the pool's own default (CPU count); *chunk* batches seeds per
    task to amortise dispatch.
    """

    start = time.perf_counter()
    with ProcessPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(run, seeds, chunksize=max(1, chunk)))
    return _aggregate(outcomes, wall_seconds=time.perf_counter() - start)


# ....................... #


def _load_simulation(target: str) -> object:
    """Resolve a ``module:attr`` import string to the object it names (inside a worker)."""

    module_name, _, attr = target.partition(":")
    if not module_name or not attr:
        raise ValueError(f"target must be 'module:attr', got {target!r}")

    module = importlib.import_module(module_name)
    return getattr(module, attr)


@final
@attrs.define(frozen=True, kw_only=True)
class SimulationSeedRunner:
    """A picklable ``run(seed)`` that imports a Simulation per worker and explores one seed.

    Holds only primitives + a ``module:attr`` string, so it pickles cleanly across processes where
    a bound :class:`~forze_dst.harness.Simulation` (full of closures and handlers) could not. Each
    call resolves the target, runs the single seed through the auto-derived scenario, and reports
    its behaviours + whether it violated — exactly what :func:`parallel_sweep` folds.
    """

    target: str
    """Import string ``module:attr`` of the :class:`~forze_dst.harness.Simulation`."""

    concurrency: int = 4
    act_count: int = 20
    perturb: bool = True
    pct: bool = False
    fault_error: float = 0.0
    latency: float = 0.0

    # ....................... #

    def _config(self, seed: int) -> SimulationConfig:
        scheduler = (
            SchedulerKind.PCT
            if self.pct
            else SchedulerKind.RANDOM
            if self.perturb
            else SchedulerKind.FIFO
        )
        faults = (
            FaultPolicy(rules=(FaultRule(error=self.fault_error),))
            if self.fault_error > 0.0
            else None
        )
        latency = (
            LatencyProfile(rules=(LatencyRule(dist=Constant(self.latency)),))
            if self.latency > 0.0
            else None
        )

        return SimulationConfig(
            seeds=[seed],
            concurrency=self.concurrency,
            act_count=self.act_count,
            scheduler=scheduler,
            faults=faults,
            latency=latency,
            coverage_plateau=0,  # a single seed — no early-stop to honour
        )

    # ....................... #

    def __call__(self, seed: int) -> SeedOutcome:
        from forze_dst.harness import (
            Simulation,
        )  # local: keep import cost in the worker

        sim = _load_simulation(self.target)
        if not isinstance(sim, Simulation):
            raise TypeError(f"{self.target!r} is not a forze_dst.Simulation")

        stats = sim.coverage(self._config(seed))
        return SeedOutcome(
            seed=seed,
            violated=stats.violation is not None,
            behaviors=stats.behaviors,
        )
