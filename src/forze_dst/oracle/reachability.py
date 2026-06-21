"""Reachability ("sometimes") assertions — the dual of an invariant.

An invariant says a property must *always* hold; a reachability target says a state
must *sometimes* be reached. They guard opposite failure modes, and DST needs both: a
sweep that runs 10k seeds and never drives the dangerous interleaving proves nothing —
a green invariant over a fault that never bit is *false confidence*. A reachability
target names a desirable hard state (a partition isolated a lock contender; a crash
landed mid-flush; a breaker tripped open) and the sweep **fails if, across all its
seeds, that state was never reached**. (Antithesis calls these "sometimes assertions"
and treats them as first-class as ordinary assertions.)

The two semantics this module gives you:

* **per-run "sometimes"** — :func:`sometimes` is true when *any* run in a sweep reached
  a state (a predicate held in ≥1 history);
* **cross-sweep "must-be-reached"** — :func:`assess_reachability` folds every run's
  reached labels into a :class:`ReachabilityReport` that knows which *declared targets*
  never fired, so a flagship scenario can assert ``report.satisfied``.

:func:`reached` marks a state from inside code under simulation — ambient and a no-op
outside a recorded run, exactly like :func:`~forze_dst.oracle.recorder.record_event`, so it is
cheap and safe to leave in scenario/handler code.
"""

from __future__ import annotations

from functools import cached_property
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping, final

import attrs

from forze_dst.oracle.recorder import History, record_event

# ----------------------- #

REACHED_KIND = "reachability"
"""The recorder event kind a reachability marker is logged under."""


def reached(label: str, **fields: Any) -> None:
    """Mark that the current run reached the notable state *label*.

    A no-op outside a recorded simulation (like :func:`~forze_dst.oracle.recorder.record_event`),
    so it is safe to leave in scenario or handler code. *label* is the reachability target a
    sweep asserts was hit at least once; *fields* are optional context for the report.
    """

    record_event(REACHED_KIND, label=label, **fields)


def reached_labels(history: History) -> frozenset[str]:
    """The set of reachability labels *history* hit."""

    return frozenset(
        str(event.fields["label"])
        for event in history.of_kind(REACHED_KIND)
        if "label" in event.fields
    )


def sometimes(
    histories: Iterable[History],
    predicate: Callable[[History], bool],
) -> bool:
    """True when *predicate* holds for at least one run — a per-sweep "sometimes" check.

    The general form behind :func:`assess_reachability`: where the latter checks named
    :func:`reached` labels, this checks an arbitrary predicate over each run's history (e.g.
    "some run observed two concurrent holders before the lock serialized them").
    """

    return any(predicate(history) for history in histories)


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class ReachabilityReport:
    """Which declared reachability targets a whole sweep reached — and which never fired."""

    targets: frozenset[str]
    """The declared targets the sweep was expected to reach at least once."""

    hits: Mapping[str, int]
    """Per label (declared or not), how many runs reached it — the observed reachability."""

    runs: int
    """How many runs were folded into this report."""

    # ....................... #

    @cached_property
    def reached(self) -> frozenset[str]:
        """Every label any run reached (whether or not it was a declared target)."""

        return frozenset(label for label, count in self.hits.items() if count > 0)

    # ....................... #

    @cached_property
    def unreached(self) -> frozenset[str]:
        """Declared targets no run reached — the reachability *failures* (the false-confidence states)."""

        return frozenset(target for target in self.targets if not self.hits.get(target))

    # ....................... #

    @property
    def satisfied(self) -> bool:
        """Whether every declared target was reached at least once across the sweep."""

        return not self.unreached

    # ....................... #

    def format(self) -> str:
        """Render a short human summary of the reachability outcome."""

        lines = [
            "DST reachability report",
            f"  runs:            {self.runs}",
            f"  targets reached: {len(self.targets) - len(self.unreached)}/{len(self.targets)}",
        ]

        for target in sorted(self.targets):
            mark = "✓" if self.hits.get(target) else "✗"
            lines.append(f"    {mark} {target}  ({self.hits.get(target, 0)} runs)")

        extra = sorted(self.reached - self.targets)
        if extra:
            lines.append(f"  also reached:    {extra}")

        return "\n".join(lines)


def assess_reachability(
    histories: Iterable[History],
    targets: Iterable[str],
) -> ReachabilityReport:
    """Fold *histories* into a :class:`ReachabilityReport` against the declared *targets*.

    Counts, across the whole sweep, how many runs reached each :func:`reached` label, and
    flags any declared target that no run ever hit. The flagship use: run a scenario over
    many seeds, assert the invariants hold *and* ``assess_reachability(...).satisfied`` — so
    a green result means the dangerous states actually fired, not that the faults never bit.
    """

    declared = frozenset(targets)
    hits: dict[str, int] = {target: 0 for target in declared}
    runs = 0

    for history in histories:
        runs += 1
        for label in reached_labels(history):
            hits[label] = hits.get(label, 0) + 1

    return ReachabilityReport(
        targets=declared,
        hits=MappingProxyType(hits),
        runs=runs,
    )
