"""Replay + minimization: find an invariant violation, then shrink it to a minimal case.

:func:`explore` searches seeds for a workload that violates an invariant; on the first
hit it *minimizes* the workload (greedy delta-debugging — remove items while the
violation survives), then returns a :class:`ViolationReport` carrying the seed, the
minimal workload, and its recorded history. Because every run is seeded and replayed on
the deterministic loop, the report reproduces exactly.
"""

from __future__ import annotations

from datetime import datetime
from typing import Awaitable, Callable, Iterable, Sequence, TypeVar, final

import attrs

from forze_dst.invariants import Invariant, Violation, check
from forze_dst.recorder import History, Recorder, bind_recorder
from forze_dst.runtime import run_simulation
from forze_dst.time_source import DEFAULT_EPOCH

# ----------------------- #

T = TypeVar("T")

Scenario = Callable[[], Awaitable[object]]
"""A zero-argument coroutine factory (call it to get a fresh coroutine to run)."""

Build = Callable[[Sequence[T]], Scenario]
"""Given a workload (sequence of items), produce a scenario that runs it with fresh
state and records domain events. Called once per run — must not share mutable state."""


def run_recorded(
    scenario: Scenario,
    *,
    seed: int,
    schedule_seed: int | None = None,
    epoch: datetime = DEFAULT_EPOCH,
) -> History:
    """Run *scenario* under the simulation loop with a bound recorder; return its history."""

    recorder = Recorder(seed=seed)

    with bind_recorder(recorder):
        run_simulation(scenario, seed=seed, schedule_seed=schedule_seed, epoch=epoch)

    return recorder.history


# ....................... #


def minimize(
    items: Sequence[T],
    still_fails: Callable[[Sequence[T]], bool],
) -> list[T]:
    """Greedily remove items while *still_fails* holds; returns a 1-minimal sublist.

    1-minimal = no single remaining item can be dropped without the failure going away.
    Order-preserving. ``O(n^2)`` evaluations worst case — fine for the small workloads
    a minimized counterexample reduces to.
    """

    current = list(items)
    changed = True

    while changed:
        changed = False
        for index in range(len(current)):
            candidate = current[:index] + current[index + 1 :]
            if candidate and still_fails(candidate):
                current = candidate
                changed = True
                break

    return current


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class ViolationReport:
    """A reproducible, minimized counterexample."""

    seed: int
    schedule_seed: int | None
    violations: tuple[Violation, ...]
    workload: tuple[object, ...]
    history: History
    registry_fingerprint: str | None = None
    """Fingerprint of the operation registry the report was found against; a replay on a
    changed registry (different fingerprint) can no longer be trusted to reproduce."""

    def format(self) -> str:
        """Render this counterexample as a readable causal report (see ``forze_dst.report``)."""

        from forze_dst.report import format_report

        return format_report(self)


def _attempt(
    build: Build[T],
    items: Sequence[T],
    invariants: Sequence[Invariant],
    *,
    seed: int,
    schedule_seed: int | None,
    epoch: datetime,
) -> ViolationReport | None:
    def run(workload: Sequence[T]) -> History:
        return run_recorded(
            build(workload), seed=seed, schedule_seed=schedule_seed, epoch=epoch
        )

    if not check(run(items), invariants):
        return None

    minimal = minimize(items, lambda subset: bool(check(run(subset), invariants)))
    final_history = run(minimal)

    return ViolationReport(
        seed=seed,
        schedule_seed=schedule_seed,
        violations=tuple(check(final_history, invariants)),
        workload=tuple(minimal),
        history=final_history,
    )


def explore(
    build: Build[T],
    items: Sequence[T],
    invariants: Sequence[Invariant],
    *,
    seeds: Iterable[int],
    perturb: bool = True,
    epoch: datetime = DEFAULT_EPOCH,
) -> ViolationReport | None:
    """Search *seeds* for an invariant violation of the full workload, then minimize it.

    With *perturb*, each seed also drives scheduler perturbation (schedule_seed = seed),
    exploring interleavings. Returns the first violating seed's minimized
    :class:`ViolationReport`, or ``None`` if no seed violated.
    """

    for seed in seeds:
        report = _attempt(
            build,
            items,
            invariants,
            seed=seed,
            schedule_seed=seed if perturb else None,
            epoch=epoch,
        )
        if report is not None:
            return report

    return None
