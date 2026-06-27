"""Commutativity assertion — the safety half of RedBlue, with no replication substrate.

Full RedBlue is a non-goal (Forze has no multi-master substrate, and nothing produces a red/blue
classification). One cheap, useful slice fits the oracle: an operation may *declare* itself
commutative (:attr:`~forze.application.execution.operations.OperationDescriptor.commutative`); this
module *verifies* the declaration. A workload built from order-independent operations must reach the
**same final state under every interleaving** — so running it once per schedule seed (the same
workload, the scheduler permuting the order) and comparing each run's final-state signature catches a
declared-commutative operation that does not actually commute. A divergence is a first-class,
reproducible finding: the offending schedule seed replays it exactly on the deterministic loop.

This is a *cross-history* checker, **not** a single-:class:`~forze_dst.oracle.recorder.History`
:data:`~forze_dst.oracle.invariants.Invariant`. Commutativity is a relation *across* interleavings:
one history is one interleaving and proves nothing on its own, so the check needs several runs and
compares their end states — it cannot be expressed as an assertion over a single history.
"""

from datetime import datetime
from typing import Callable, Hashable, Iterable

from ..time_source import DEFAULT_EPOCH
from .invariants import Violation
from .recorder import History
from .replay import Scenario, run_recorded

# ----------------------- #


def commutative_convergence(
    build: Callable[[], Scenario],
    *,
    final_state: Callable[[History], Hashable],
    schedule_seeds: Iterable[int],
    seed: int = 0,
    epoch: datetime = DEFAULT_EPOCH,
) -> list[Violation]:
    """A declared-commutative workload must reach one final state across all interleavings.

    Calls *build* once per entry in *schedule_seeds* for a **fresh** scenario (fresh state — the same
    no-shared-mutable-state rule as :data:`~forze_dst.oracle.replay.Build`, since the workload runs
    several times) and runs it: the input ``seed`` is fixed so generation is identical, and only the
    scheduler permutes the interleaving. Each run's final-state signature is extracted via
    *final_state* — whatever end state must agree for the operations to commute: the RFC's "compare
    end-states by touched keys" (the committed value per key), but any hashable projection of the
    history works (a recorded final marker, a register's last value, …). If two interleavings land in
    different signatures, a declared-commutative operation did not actually commute.

    Returns one :class:`Violation` per *distinct extra* final state (the first seed's state is the
    baseline), each naming the schedule seed that reproduces it. Empty == every interleaving
    converged, so the commutativity declaration holds over the explored band (sound: a divergence is
    a genuine counterexample; incomplete: only the interleavings these seeds reach are tried — widen
    the band, or drive it from the coverage-guided explorer, to hunt harder).
    """

    first_seed_for: dict[Hashable, int] = {}

    for schedule_seed in schedule_seeds:
        history = run_recorded(
            build(), seed=seed, schedule_seed=schedule_seed, epoch=epoch
        )
        first_seed_for.setdefault(final_state(history), schedule_seed)

    if len(first_seed_for) <= 1:
        return []

    (baseline_state, baseline_seed), *divergent = first_seed_for.items()

    return [
        Violation(
            invariant="commutative",
            message=(
                f"reordering diverged: schedule_seed={schedule_seed} reached final state "
                f"{state!r}, but schedule_seed={baseline_seed} reached {baseline_state!r} — a "
                "declared-commutative workload is not order-independent"
            ),
        )
        for state, schedule_seed in divergent
    ]
