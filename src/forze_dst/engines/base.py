"""Shared explore/attempt skeleton for the exploration engines.

The seed sweep and the find → minimize → report tail are identical across the op-case, scenario,
and crash/restart engines; only *how one attempt runs* differs (which workload, which ``run_*``).
Each engine supplies that as closures; this module owns the loop and the minimization, so a fix to
the minimize/report logic applies everywhere and a new engine only writes its "run one attempt".
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Sequence, TypeVar

from forze.base.primitives import derive_seed
from forze_dst.oracle import ViolationReport, minimize
from forze_dst.oracle.invariants import check
from forze_dst.oracle.recorder import History

if TYPE_CHECKING:
    from forze_dst.harness import Simulation

# ----------------------- #

T = TypeVar("T")
"""The engine's workload item type (an operation ``Call``, an ``(op, arg)`` act, …)."""


def explore_seeds(
    seeds: Sequence[int],
    attempt_fn: Callable[[int], "ViolationReport | None"],
) -> "ViolationReport | None":
    """Run *attempt_fn* over *seeds*; return the first violating seed's report (else ``None``)."""

    for seed in seeds:
        report = attempt_fn(seed)

        if report is not None:
            return report

    return None


def scheduler_for(
    seed: int, factory: Callable[[int], object] | None
) -> object | None:
    """A fresh per-seed scheduler from *factory* (seeded with the schedule sub-seed), or ``None``.

    Stateful schedulers (PCT) must be rebuilt per run so the initial run, every minimization
    predicate, and the final replay all explore the SAME interleaving — otherwise a counterexample
    minimized against a mutated schedule fails to reproduce from the reported seed.
    """

    return None if factory is None else factory(derive_seed(seed, "schedule"))


def attempt_and_minimize(
    sim: "Simulation",
    *,
    seed: int,
    schedule_seed: int | None,
    run_initial: Callable[[], tuple[History, Sequence[T]]],
    run_subset: Callable[[Sequence[T]], History],
    format_workload: Callable[[Sequence[T]], tuple[Any, ...]],
) -> "ViolationReport | None":
    """The shared find → minimize → report tail.

    Run the initial attempt (*run_initial* returns its history and workload); if no invariant is
    violated, return ``None``. Otherwise greedily minimize the workload to a still-failing subset
    (re-running each candidate via *run_subset*), replay the minimal workload, and build the
    reproducible :class:`ViolationReport` with *format_workload* serializing the workload.
    """

    history, workload = run_initial()

    if not check(history, sim.invariants):
        return None

    minimal = minimize(
        workload, lambda subset: bool(check(run_subset(subset), sim.invariants))
    )
    final_history = run_subset(minimal)

    return ViolationReport(
        seed=seed,
        schedule_seed=schedule_seed,
        violations=tuple(check(final_history, sim.invariants)),
        workload=format_workload(minimal),
        history=final_history,
        registry_fingerprint=sim.fingerprint(),
    )
