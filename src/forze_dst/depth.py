"""Mechanical bug-depth extraction — the locked ``d = 1 + |1-minimal non-FIFO choices|``.

The corpus's depth labels are the axis the seed-statistics experiments plot over, so they must be
*derived*, not asserted: find a violating interleaving of a fixed workload with the systematic
(DPOR-family) explorer, then greedily zero its scheduling choices while the violation still
reproduces. The surviving non-FIFO choices are the ordering constraints the bug genuinely needs;
their count plus one is ``d``. The evidence (the minimized vector and the workload seed it
reproduces under) travels with the label so a reviewer can re-derive it.

**Scope of the PCT correspondence.** This ``d`` counts deviations from the *cooperative
round-robin* baseline (one promoted ready-callback per non-FIFO tick); the ``depth`` parameter
of :class:`~forze_dst.scheduler.PCTReorderer` counts priority-change points against a
*run-to-priority* baseline. The two coincide at ``d <= 2`` (one promotion ↔ one change point)
but can diverge beyond it in either direction — a bug needing two separated stalls of one task
is two promotions here yet three change points for PCT, and a single long stall is one change
point yet many promotions. Measured-p̂-versus-PCT-bound comparisons for ``d >= 3`` labels remain
valid in the conservative direction (the floor shrinks as ``k^(d-1)``), but a label should be
accompanied by a per-strategy detection-density probe before it is read as "what PCT-d
buys" — the correspondence is an empirical question there, not a definition.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import final

import attrs

from forze_dst.engines.scenario import explore_dpor, run_scenario
from forze_dst.misuse import MisuseCase
from forze_dst.oracle.invariants import check
from forze_dst.scheduler import SystematicReorderer
from forze_dst.time_source import DEFAULT_EPOCH

# ----------------------- #


@final
@attrs.frozen(kw_only=True)
class DepthEvidence:
    """A mechanically derived depth label: the 1-minimal choice vector and how to re-derive it."""

    depth: int
    choices: tuple[int, ...]
    """The 1-minimal non-FIFO choice vector (trailing FIFO zeros trimmed): no single remaining
    non-zero choice can be zeroed without the violation vanishing."""

    seed: int
    """The workload seed the vector reproduces under (the workload is fixed; only order varies)."""

    act_count: int
    concurrency: int

    # ....................... #

    def __attrs_post_init__(self) -> None:
        nonzero = sum(1 for choice in self.choices if choice)
        if self.depth != 1 + nonzero:
            raise ValueError(
                f"depth {self.depth} does not match 1 + {nonzero} non-FIFO choice(s) "
                f"in {self.choices!r}"
            )

    # ....................... #

    def note(self) -> str:
        """The registry-ready evidence string."""

        return (
            f"mechanical: d = 1 + {sum(1 for c in self.choices if c)} non-FIFO choice(s) in the "
            f"1-minimal schedule {self.choices!r} (workload seed {self.seed}, "
            f"act_count={self.act_count}, concurrency={self.concurrency})"
        )


# ....................... #


def _one_minimal(found: Sequence[int], still_fails: Callable[[Sequence[int]], bool]) -> list[int]:
    """Greedily zero *found*'s choices to fixpoint (1-minimal), then trim trailing FIFO zeros.

    1-minimal: no single remaining non-zero choice can be zeroed without the violation
    (as judged by *still_fails*) vanishing.
    """

    choices = list(found)
    changed = True
    while changed:
        changed = False
        for index, value in enumerate(choices):
            if value == 0:
                continue
            candidate = list(choices)
            candidate[index] = 0
            if still_fails(candidate):
                choices = candidate
                changed = True

    while choices and choices[-1] == 0:
        choices.pop()

    return choices


# ....................... #


def extract_depth(
    case: MisuseCase,
    *,
    act_count: int,
    concurrency: int,
    seeds: Sequence[int] = range(20),
    max_runs: int = 2000,
) -> DepthEvidence:
    """Derive the depth label for a corpus case under the given workload knobs.

    Walks *seeds* until the systematic explorer finds a violating interleaving of that seed's
    fixed workload, then greedily zeroes scheduling choices (to fixpoint — 1-minimal) with the
    violation re-checked after every step. Raises when no seed's schedule tree yields a violation
    within *max_runs* — a corpus case the systematic explorer cannot kill needs a manual label
    with its own justification, never a silent default.
    """

    if case.scenario is None:
        raise ValueError("depth extraction needs the case's explicit scenario")

    simulation, scenario = case.simulation, case.scenario

    found_seed: int | None = None
    found_choices: tuple[int, ...] | None = None

    for seed in seeds:
        report = explore_dpor(
            simulation,
            scenario,
            act_count=act_count,
            concurrency=concurrency,
            seed=seed,
            max_runs=max_runs,
        )
        if report is not None and report.choices is not None:
            found_seed, found_choices = seed, report.choices
            break

    if found_seed is None or found_choices is None:
        raise RuntimeError(
            f"no violating interleaving within {max_runs} systematic runs over seeds {seeds!r}"
        )

    # Re-derive the fixed workload exactly as the explorer did (same seed, same generation path).
    _, workload = run_scenario(
        simulation,
        scenario,
        act_workload=None,
        act_count=act_count,
        concurrency=concurrency,
        seed=found_seed,
        schedule_seed=None,
        epoch=DEFAULT_EPOCH,
    )

    def still_fails(vector: Sequence[int]) -> bool:
        history, _ = run_scenario(
            simulation,
            scenario,
            act_workload=workload,
            act_count=act_count,
            concurrency=concurrency,
            seed=found_seed,
            schedule_seed=None,
            epoch=DEFAULT_EPOCH,
            scheduler=SystematicReorderer(tuple(vector)),
        )
        return bool(check(history, simulation.invariants))

    choices = _one_minimal(found_choices, still_fails)

    return DepthEvidence(
        depth=1 + sum(1 for value in choices if value),
        choices=tuple(choices),
        seed=found_seed,
        act_count=act_count,
        concurrency=concurrency,
    )
