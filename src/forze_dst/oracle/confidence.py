"""Confidence — what a sweep actually exercised, so a green result means something.

A clean run only proves what was *tried*. A sweep can pass every invariant while never driving
the dangerous case — and "no violation" then reads as safety when it is really silence. This
reads the recorded histories and surfaces the gaps that make green less reassuring than it looks:

* **Operations that ran but never raced** — an op that always ran alone had its concurrency
  *checked against nothing*. Its happy path passed; its interleavings were never explored.
* **Declared faults that never fired** — a fault rule no seed ever triggered means that failure
  path was never exercised, so a green result says nothing about it.

It reuses the oracle's existing structure: :meth:`~forze_dst.oracle.report.CausalGraph.concurrent_groups`
for the overlap relation and the injected-environment timeline for what actually fired.
"""

from __future__ import annotations

from collections.abc import Iterable
from functools import cached_property
from typing import TYPE_CHECKING, Any, final

import attrs

from forze_dst.oracle.recorder import History
from forze_dst.oracle.report import CausalGraph

if TYPE_CHECKING:
    from forze_dst.faults import FaultPolicy

# ----------------------- #


def _fault_label(rule: Any) -> str:
    """A stable, readable identifier for a declared fault rule — its selector + active kinds."""

    selector = f"{rule.surface or '*'}[{rule.route or '*'}].{rule.op or '*'}"
    kinds = [
        kind
        for kind in ("error", "timeout", "crash", "drop", "duplicate", "delay")
        if getattr(rule, kind) > 0.0
    ]

    return f"{selector} ({'/'.join(kinds)})" if kinds else selector


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class ConfidenceReport:
    """What a sweep exercised — the basis for trusting (or distrusting) a green result.

    :attr:`never_raced` and :attr:`faults_never_fired` are the gaps: anything there passed only
    because it was never truly tested. :attr:`clean` is ``True`` when neither has entries.
    """

    seeds_run: int
    """How many seeds the sweep ran."""

    ran_ops: tuple[str, ...]
    """Every operation that ran at least once across the sweep."""

    raced_ops: tuple[str, ...]
    """Operations that overlapped at least one other operation at least once (genuine concurrency)."""

    faults_declared: tuple[str, ...] = ()
    """The fault rules the config declared (rendered by selector + kinds)."""

    faults_fired: tuple[str, ...] = ()
    """The declared fault rules that actually triggered on some seed."""

    # ....................... #

    @cached_property
    def never_raced(self) -> tuple[str, ...]:
        """Operations that ran but never overlapped another — their concurrency was untested."""

        raced = set(self.raced_ops)
        return tuple(op for op in self.ran_ops if op not in raced)

    # ....................... #

    @cached_property
    def faults_never_fired(self) -> tuple[str, ...]:
        """Declared fault rules that no seed ever triggered — that failure path was unexercised."""

        fired = set(self.faults_fired)
        return tuple(label for label in self.faults_declared if label not in fired)

    # ....................... #

    @cached_property
    def warnings(self) -> tuple[str, ...]:
        """Human one-liners for each confidence gap (empty when the sweep left none)."""

        out: list[str] = []

        if self.never_raced:
            out.append(
                f"ran but never raced: {', '.join(self.never_raced)} "
                "— their concurrency was never tested"
            )

        if self.faults_never_fired:
            out.append(
                f"declared fault never fired: {', '.join(self.faults_never_fired)} "
                "— that failure path was never exercised"
            )

        return tuple(out)

    # ....................... #

    @property
    def clean(self) -> bool:
        """Whether every operation raced and every declared fault fired — no untested gap."""

        return not self.warnings

    # ....................... #

    def format(self) -> str:
        """Render a short human summary — what a green run did and didn't exercise."""

        lines = [
            "DST confidence",
            f"  seeds run:    {self.seeds_run}",
            f"  raced:        {len(self.raced_ops)}/{len(self.ran_ops)} operations overlapped another",
        ]

        if self.faults_declared:
            lines.append(
                f"  faults fired: {len(self.faults_fired)}/{len(self.faults_declared)} declared rules"
            )

        if self.warnings:
            lines.append("  ⚠ confidence gaps:")
            lines.extend(f"      • {warning}" for warning in self.warnings)
        else:
            lines.append("  ✓ every operation raced and every declared fault fired")

        return "\n".join(lines)


# ....................... #


@attrs.define
class ConfidenceProbe:
    """Folds per-seed histories into the cross-sweep confidence signals, one history at a time.

    Incremental by design — the sweep feeds each history in as it runs, so thousands of seeds
    never have to be held in memory at once.
    """

    _ran: set[str] = attrs.field(factory=set)
    _raced: set[str] = attrs.field(factory=set)
    _fired_calls: set[tuple[Any, Any, str]] = attrs.field(factory=set)
    _seeds: int = 0

    # ....................... #

    def observe(self, history: History) -> None:
        """Fold one recorded run into the accumulators."""

        self._seeds += 1
        graph = CausalGraph.from_history(history)

        for span in graph.spans:
            self._ran.add(span.op)

        for group in graph.concurrent_groups():
            for span in group:
                self._raced.add(span.op)

        for event in graph.timeline:
            if event.kind == "fault":
                self._fired_calls.add(
                    (
                        event.fields.get("surface"),
                        event.fields.get("route"),
                        str(event.fields.get("op")),
                    )
                )

    # ....................... #

    def report(self, *, faults: FaultPolicy | None = None) -> ConfidenceReport:
        """Build the report; *faults* is the declared policy whose rules are checked for firing."""

        declared: list[str] = []
        fired: list[str] = []

        if faults is not None:
            for rule in faults.rules:
                label = _fault_label(rule)
                declared.append(label)
                if any(
                    rule.matches_parts(surface, route, op)
                    for surface, route, op in self._fired_calls
                ):
                    fired.append(label)

        return ConfidenceReport(
            seeds_run=self._seeds,
            ran_ops=tuple(sorted(self._ran)),
            raced_ops=tuple(sorted(self._raced)),
            faults_declared=tuple(declared),
            faults_fired=tuple(fired),
        )


# ....................... #


def assess_confidence(
    histories: Iterable[History], *, faults: FaultPolicy | None = None
) -> ConfidenceReport:
    """Read a sweep's recorded *histories* into a :class:`ConfidenceReport`.

    *faults* is the config's declared :class:`~forze_dst.faults.FaultPolicy` (if any), so the
    report can name the rules that were declared but never triggered. Folds the histories in one
    pass, so an iterator/generator works without materializing every run.
    """

    probe = ConfidenceProbe()
    for history in histories:
        probe.observe(history)

    return probe.report(faults=faults)
