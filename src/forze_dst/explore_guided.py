"""Coverage-guided MUTATION exploration (finishing S7) — feedback-directed input generation.

A uniform seed sweep knows when to *stop* (coverage plateaus) but not *where to look*: every run
is independent, so a behavior gated behind a rare combination of operations is found only by luck.
Coverage-guided mutation (AFL / Antithesis) closes that gap — keep a **corpus** of inputs that each
unlocked new behavior, **mutate** a corpus entry (tweak an op, grow/shrink the workload, re-roll the
schedule + faults), and keep the child only if it adds new behavioral coverage *or* trips an
invariant. An AFL-style **power schedule** spends energy on the most productive, least-exercised
entries, so exploration drifts toward the frontier instead of sampling blindly.

Determinism holds end to end: every choice — which corpus entry, which mutation, the re-roll
sub-seed — is drawn from one RNG seeded off the master seed, so the whole guided run (corpus and
all) reproduces from that single seed. The engine is substrate-free: it is handed a ``run`` that
turns a :class:`Genome` into a :class:`~forze_dst.oracle.recorder.History`, so it can be unit-tested on a
synthetic oracle and reused over any harness.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from typing import TYPE_CHECKING, final

import attrs

from forze.base.primitives import derive_seed
from forze_dst.oracle.coverage import Behavior, behavioral_coverage
from forze_dst.oracle.recorder import History

if TYPE_CHECKING:
    from forze_dst.oracle import ViolationReport

# ----------------------- #


@final
@attrs.define(frozen=True, kw_only=True)
class Genome:
    """One fuzzer input: the workload (op indices into the catalog) plus the run's sub-seed.

    ``ops`` is the explicit, mutable workload — index ``i`` selects ``catalog[i]``. ``seed`` drives
    every *other* nondeterminism stream of the run (schedule, faults, latency, generated inputs)
    through the usual ``derive_seed`` sub-seeds, so a re-seed mutation explores fresh
    interleavings/faults while the op sequence holds. The pair fully determines the run, so a
    corpus of genomes replays exactly.
    """

    ops: tuple[int, ...]
    seed: int


# ....................... #


def mutate(
    genome: Genome,
    rng: random.Random,
    *,
    catalog_size: int,
    max_ops: int,
) -> Genome:
    """Apply one random mutation to *genome*, drawn from *rng* (so the lineage is seed-derived).

    The mutation set: ``point`` (retarget one op), ``insert`` / ``delete`` (grow or shrink the
    workload within ``[1, max_ops]``), and ``reseed`` (re-roll the schedule/fault/input streams
    while holding the op sequence). Small, local edits — the property AFL relies on: a child stays
    close to a productive parent, so coverage compounds instead of restarting.
    """

    ops = list(genome.ops)
    kinds = ["reseed"]
    if len(ops) < max_ops:
        kinds.append("insert")
    if ops:
        kinds.append("point")
    if len(ops) > 1:
        kinds.append("delete")

    kind = rng.choice(kinds)

    if kind == "point":
        ops[rng.randrange(len(ops))] = rng.randrange(catalog_size)
        return Genome(ops=tuple(ops), seed=genome.seed)

    if kind == "insert":
        ops.insert(rng.randrange(len(ops) + 1), rng.randrange(catalog_size))
        return Genome(ops=tuple(ops), seed=genome.seed)

    if kind == "delete":
        del ops[rng.randrange(len(ops))]
        return Genome(ops=tuple(ops), seed=genome.seed)

    return Genome(ops=genome.ops, seed=rng.getrandbits(32))  # reseed


# ....................... #


@final
@attrs.define
class _Entry:
    """A corpus member: a genome that unlocked new coverage, with its AFL energy bookkeeping."""

    genome: Genome
    contribution: int
    """How many behaviors this genome first unlocked — its base energy."""
    uses: int = 0
    """How many times it has been chosen as a mutation parent — energy decays with use."""


def _pick(corpus: list[_Entry], rng: random.Random) -> _Entry:
    """Power schedule: favor productive, recently-found, least-exercised entries (AFL energy).

    Weight rises with an entry's contribution and its **recency** (later corpus index — the newest
    coverage *frontier*, which is where fresh behavior is most likely to be one mutation away) and
    decays with how often it has already been mutated, so exploration pushes the frontier outward
    instead of re-sampling exhausted inputs uniformly.
    """

    weights = [
        entry.contribution * (index + 1) / (1.0 + entry.uses) for index, entry in enumerate(corpus)
    ]
    return rng.choices(corpus, weights=weights, k=1)[0]


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class GuidedStats:
    """The outcome of a coverage-guided run: how much behavior was reached, and any bug found."""

    behaviors: frozenset[Behavior]
    """Every distinct behavior the guided run exercised across all its inputs."""

    runs: int
    """How many inputs were executed (the spent budget)."""

    corpus_size: int
    """How many inputs made it into the corpus (each unlocked new behavior)."""

    new_by_run: tuple[int, ...]
    """Per run, in order: the count of behaviors it added that no earlier run had."""

    violation: ViolationReport | None = None
    """The minimized counterexample, if a run tripped an invariant (the guided run stops there)."""

    # ....................... #

    @property
    def size(self) -> int:
        """The number of distinct behaviors covered."""

        return len(self.behaviors)

    # ....................... #

    def format(self) -> str:
        """Render a short human summary of the guided exploration."""

        lines = [
            "DST coverage-guided report",
            f"  behaviors covered: {self.size}",
            f"  runs (budget):     {self.runs}",
            f"  corpus entries:    {self.corpus_size}",
        ]

        if self.violation is not None:
            names = ", ".join(sorted({v.invariant for v in self.violation.violations}))
            lines.append(f"  ✗ violation at seed {self.violation.seed}: {names}")

        return "\n".join(lines)


# ....................... #


def coverage_guided_search(
    *,
    seed_genome: Genome,
    run: Callable[[Genome], History],
    is_violation: Callable[[History], bool],
    on_violation: Callable[[Genome], ViolationReport | None],
    master_seed: int,
    budget: int,
    catalog_size: int,
    max_ops: int,
) -> GuidedStats:
    """Drive a coverage-guided mutation loop from *seed_genome* up to *budget* runs.

    Seeds the corpus with *seed_genome*, then repeatedly picks a corpus entry by the power
    schedule, mutates it, and runs the child: a child that adds new behavioral coverage joins the
    corpus, and one that trips an invariant ends the run with a minimized report (via
    *on_violation*). Every random choice derives from *master_seed*, so the run reproduces. *run*
    maps a genome to its recorded history; *is_violation* reads that history; *catalog_size* and
    *max_ops* bound the mutation space.
    """

    rng = random.Random(derive_seed(master_seed, "guided"))  # nosec B311 - seeded fuzz lineage
    behaviors: set[Behavior] = set()
    new_by_run: list[int] = []
    corpus: list[_Entry] = []

    # Seed the corpus — its behavior is the exploration's baseline coverage.
    history = run(seed_genome)
    if is_violation(history):
        return GuidedStats(
            behaviors=frozenset(),
            runs=1,
            corpus_size=0,
            new_by_run=(0,),
            violation=on_violation(seed_genome),
        )

    covered = behavioral_coverage(history)
    behaviors |= covered
    new_by_run.append(len(covered))
    corpus.append(_Entry(genome=seed_genome, contribution=max(1, len(covered))))

    runs = 1

    while runs < budget:
        parent = _pick(corpus, rng)
        parent.uses += 1
        child = mutate(parent.genome, rng, catalog_size=catalog_size, max_ops=max_ops)

        history = run(child)
        runs += 1

        if is_violation(history):
            return GuidedStats(
                behaviors=frozenset(behaviors),
                runs=runs,
                corpus_size=len(corpus),
                new_by_run=tuple(new_by_run),
                violation=on_violation(child),
            )

        covered = behavioral_coverage(history)
        fresh = covered - behaviors
        new_by_run.append(len(fresh))

        if fresh:
            behaviors |= covered
            corpus.append(_Entry(genome=child, contribution=len(fresh)))

    return GuidedStats(
        behaviors=frozenset(behaviors),
        runs=runs,
        corpus_size=len(corpus),
        new_by_run=tuple(new_by_run),
        violation=None,
    )


# ....................... #

__all__ = [
    "Genome",
    "GuidedStats",
    "coverage_guided_search",
    "mutate",
]
