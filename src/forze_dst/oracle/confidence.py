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

from collections import Counter
from collections.abc import Iterable, Mapping
from functools import cached_property
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, final

import attrs

from forze_dst.oracle.coverage import NO_SHAPES, behavioral_fingerprint
from forze_dst.oracle.horizon import HorizonAnalysis, HorizonProbe
from forze_dst.oracle.recorder import History
from forze_dst.oracle.report import CausalGraph
from forze_dst.stats import (
    CoverageDeficit,
    coverage_deficit,
    format_clean_verdict,
    format_coverage_deficit,
    format_withheld_verdict,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

    from forze_dst.faults import FaultPolicy
    from forze_dst.oracle.invariants import Invariant
    from forze_dst.oracle.witness import InvariantAccounting

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

    violations_seen: int = 0
    """How many violating seeds the sweep hit (``0`` = clean). The quantitative clean-run bound
    prints only when the sweep was actually clean — this field is how the report knows."""

    vacuous_invariants: tuple[tuple[str, tuple[str, ...]], ...] = ()
    """``(invariant, read kinds)`` pairs the horizon analysis flagged vacuous: no run ever
    recorded any kind the invariant reads, so it was never at risk — its green is a constant."""

    marker_blind_invariants: tuple[tuple[str, tuple[str, ...]], ...] = ()
    """``(invariant, kinds)`` pairs folding handler-emitted markers recorded while a transaction
    that later rolled back was in flight — the rollback blind spot (a marker survives rollback;
    the port effects it claims to witness do not)."""

    accounting: InvariantAccounting | None = None
    """Per-invariant witness accounting, when the simulation opted in (declared witnesses or
    horizon declarations). Makes the verdict's oracle-set clause countable."""

    data_dependent_stop: str | None = None
    """The rule that ended the sweep early by *looking at the runs*, or ``None`` when the seed
    count was fixed before any of them existed. A bound is a statement about a denominator, and
    Clopper–Pearson's exactness is a fixed-design guarantee — under a data-dependent stopping
    rule ``seeds_run`` is a random variable whose value depends on the runs being summarized, so
    :meth:`verdict` states the stop reason and withholds the number."""

    shape_counts: Mapping[str, int] = NO_SHAPES
    """Execution-shape fingerprint → how many of the swept runs produced it. The frequency table
    :attr:`deficit` extrapolates from; empty when the sweep folded no runs."""

    # ....................... #

    @cached_property
    def deficit(self) -> CoverageDeficit | None:
        """How much of the execution-shape alphabet the sweep did *not* reach (``None`` when it
        folded no runs). Saturation measured rather than asserted — see
        :func:`~forze_dst.stats.coverage_deficit` for why this runs on shapes and not on the
        behaviour alphabet."""

        return coverage_deficit(self.shape_counts) if self.shape_counts else None

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

        out.extend(
            f"vacuous invariant: {name} reads {', '.join(kinds)} but no run ever recorded "
            "those kinds — it was never at risk; fix the scenario or drop the invariant"
            for name, kinds in self.vacuous_invariants
        )

        out.extend(
            f"marker-blind invariant: {name} folds handler-emitted marker(s) "
            f"{', '.join(kinds)} recorded while a transaction that rolled back was in "
            "flight — prefer a port-state oracle (read the store in observe)"
            for name, kinds in self.marker_blind_invariants
        )

        if self.accounting is not None:
            out.extend(self.accounting.problems)

        return tuple(out)

    # ....................... #

    @property
    def clean(self) -> bool:
        """Whether every operation raced and every declared fault fired — no untested gap."""

        return not self.warnings

    # ....................... #

    def verdict(self, runs: int | None = None) -> str:
        """The locked clean-run verdict for this sweep, accounting-scoped when available.

        The single place the countable clause is threaded, so every surface that prints the
        verdict (this report, the coverage report, the CLI) states the identical claim — including
        the refusal to print one at all when :attr:`data_dependent_stop` says ``n`` came from the
        data.
        """

        seeds = self.seeds_run if runs is None else runs

        if self.data_dependent_stop is not None:
            return format_withheld_verdict(seeds, stop_reason=self.data_dependent_stop)

        accounting = self.accounting

        if accounting is None:
            return format_clean_verdict(seeds)

        return format_clean_verdict(
            seeds,
            witnessed=len(accounting.witnessed),
            declared=accounting.declared,
            unexercisable=accounting.unexercisable,
            unaccounted=accounting.unaccounted,
        )

    # ....................... #

    def format(self) -> str:
        """Render a short human summary — what a green run did and didn't exercise."""

        stop = f"  ({self.data_dependent_stop})" if self.data_dependent_stop is not None else ""
        lines = [
            "DST confidence",
            f"  seeds run:    {self.seeds_run}{stop}",
            f"  raced:        {len(self.raced_ops)}/{len(self.ran_ops)} operations overlapped another",
        ]

        deficit = self.deficit
        if deficit is not None:
            lines.append(f"  exec. shapes: {format_coverage_deficit(deficit)}")

        if self.faults_declared:
            lines.append(
                f"  faults fired: {len(self.faults_fired)}/{len(self.faults_declared)} declared rules"
            )

        if self.accounting is not None:
            line = (
                f"  invariants:   {len(self.accounting.witnessed)} witnessed / "
                f"{len(self.accounting.declared)} declared out-of-horizon / "
                f"{len(self.accounting.unaccounted)} unaccounted"
            )
            if self.accounting.unexercisable:
                line += f" / {len(self.accounting.unexercisable)} unexercisable under this config"
            lines.append(line)

        if self.warnings:
            lines.append("  ⚠ confidence gaps:")
            lines.extend(f"      • {warning}" for warning in self.warnings)
        else:
            lines.append("  ✓ every operation raced and every declared fault fired")

        # The quantitative verdict sits adjacent to the gaps so the bound is never read as
        # stronger than the coverage supports. A withheld verdict is not a positive result and
        # does not get the tick.
        if self.violations_seen == 0 and self.seeds_run > 0:
            mark = "⚠" if self.data_dependent_stop is not None else "✓"
            lines.append(f"  {mark} {self.verdict()}")

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
    _horizon: HorizonProbe = attrs.field(factory=HorizonProbe)
    _shapes: Counter[str] = attrs.field(factory=Counter)
    _seeds: int = 0

    # ....................... #

    def observe(self, history: History) -> None:
        """Fold one recorded run into the accumulators."""

        self._seeds += 1
        self._horizon.observe(history)
        # One counter bucket per run — the frequency-of-frequencies the deficit estimator needs
        # survives, while the histories themselves still do not have to.
        self._shapes[behavioral_fingerprint(history)] += 1
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

    def report(
        self,
        *,
        faults: FaultPolicy | None = None,
        violations: int = 0,
        invariants: Sequence[Invariant] = (),
        accounting: InvariantAccounting | None = None,
        data_dependent_stop: str | None = None,
    ) -> ConfidenceReport:
        """Build the report; *faults* is the declared policy whose rules are checked for firing.

        *violations* is how many violating seeds the sweep hit — the probe itself only sees
        histories, so the caller supplies it; it gates the clean-run bound in the report.
        *invariants* enables the horizon analysis (vacuity / marker-blindness flags over the
        folded histories); *accounting* is the simulation's witness accounting, threaded so the
        verdict's oracle-set clause is countable. *data_dependent_stop* names the rule that ended
        the sweep by reading the runs (the caller knows the design; the probe only sees
        histories), which withholds the bound.
        """

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

        horizon = self._horizon.analyze(invariants) if invariants else HorizonAnalysis()

        return ConfidenceReport(
            seeds_run=self._seeds,
            ran_ops=tuple(sorted(self._ran)),
            raced_ops=tuple(sorted(self._raced)),
            faults_declared=tuple(declared),
            faults_fired=tuple(fired),
            violations_seen=violations,
            vacuous_invariants=horizon.vacuous,
            marker_blind_invariants=horizon.marker_blind,
            accounting=accounting,
            data_dependent_stop=data_dependent_stop,
            shape_counts=MappingProxyType(dict(self._shapes)),
        )


# ....................... #


def assess_confidence(
    histories: Iterable[History],
    *,
    faults: FaultPolicy | None = None,
    violations: int = 0,
    invariants: Sequence[Invariant] = (),
    accounting: InvariantAccounting | None = None,
    data_dependent_stop: str | None = None,
) -> ConfidenceReport:
    """Read a sweep's recorded *histories* into a :class:`ConfidenceReport`.

    *faults* is the config's declared :class:`~forze_dst.faults.FaultPolicy` (if any), so the
    report can name the rules that were declared but never triggered. *violations* is how many
    of those runs violated (histories alone can't tell — it gates the clean-run bound). Folds
    the histories in one pass, so an iterator/generator works without materializing every run.
    *invariants* / *accounting* / *data_dependent_stop* enable the horizon analysis, the countable
    verdict clause, and the withheld-bound refusal (see :meth:`ConfidenceProbe.report`).
    """

    probe = ConfidenceProbe()
    for history in histories:
        probe.observe(history)

    return probe.report(
        faults=faults,
        violations=violations,
        invariants=invariants,
        accounting=accounting,
        data_dependent_stop=data_dependent_stop,
    )
