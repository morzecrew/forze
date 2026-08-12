"""Coverage-guided exploration (S7) — a behavioral coverage signal over a run's history.

A uniform seed sweep wastes effort once behavior saturates and gives no signal for *how much*
of the app it actually exercised. :func:`behavioral_coverage` distills a run's converged
history into the set of distinct *behaviors* it touched — each operation's ``(op, outcome)``,
each port edge ``(domain, surface, route, op, phase, outcome)``, and each injected
fault/partition kind. It is structural and PII-free (no entity keys or values), so two runs
that drove the same shapes of behavior have the same coverage even with different ids.

The harness feeds this to :meth:`~forze_dst.harness.Simulation.coverage`: sweep seeds while
coverage keeps growing, stop once it plateaus (Antithesis-style — spend effort where new
behavior is still appearing), and report which seeds mattered.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from functools import cached_property
from types import MappingProxyType
from typing import TYPE_CHECKING, final

import attrs

from forze_dst.oracle.recorder import History
from forze_dst.stats import (
    CoverageDeficit,
    coverage_deficit,
    format_clean_verdict,
    format_coverage_deficit,
    format_withheld_verdict,
)

if TYPE_CHECKING:
    from forze_dst.oracle import ViolationReport
    from forze_dst.oracle.confidence import ConfidenceReport
    from forze_dst.oracle.reachability import ReachabilityReport

# ----------------------- #

Behavior = tuple[str | None, ...]
"""One distinct, PII-free behavioral feature exercised by a run (an operation outcome, a port
edge, or an injected fault/partition kind). A tag string followed by the structural tokens of the
feature — each a recorded field that is a string or ``None``, never an id, key, or payload."""

NO_SHAPES: Mapping[str, int] = MappingProxyType({})
"""The empty execution-shape table — a shared immutable default, so a frozen report never owns a
mutable one."""


def behavioral_coverage(history: History) -> frozenset[Behavior]:
    """The set of distinct behaviors *history* exercised — the run's coverage fingerprint.

    Structural only: operation ``(op, outcome)``, port edges (domain / surface / route / op /
    phase / outcome), and injected fault/partition kinds. No entity keys or payloads, so
    coverage compares across runs regardless of ids.
    """

    coverage: set[Behavior] = set()

    for event in history.events:
        fields = event.fields

        if event.kind == "operation":
            coverage.add(("op", fields.get("op"), fields.get("outcome")))

        elif event.kind == "trace":
            coverage.add(
                (
                    "edge",
                    fields.get("trace_domain"),
                    fields.get("surface"),
                    fields.get("route"),
                    fields.get("op"),
                    fields.get("phase"),
                    fields.get("outcome"),
                )
            )

        elif event.kind in ("fault", "partition"):
            coverage.add(
                (
                    "env",
                    event.kind,
                    fields.get("fault"),
                    fields.get("surface"),
                    fields.get("op"),
                )
            )

    return frozenset(coverage)


# ....................... #


def behavioral_fingerprint(history: History) -> str:
    """A stable digest of the run's execution-trace *shape* — its handler-logic signature.

    Where :func:`behavioral_coverage` is an unordered *set* of behaviors ("did we ever see X?"),
    this is an *ordered*, PII-free signature of the whole run: the sequence of operation outcomes
    and port edges, with ids / keys / values / timing excluded. Two runs whose handlers drove the
    same path get the same fingerprint; a handler-logic change — a different port call, a reordered
    write, a changed outcome — changes it. That lets a replay flag *behavioral drift*: an old seed
    re-run against code whose logic moved on, even when the structural operation-catalog
    fingerprint is unchanged (same contracts, different behavior). Opt-in by design — a bytecode
    digest would be brittle; this captures observed behavior, not source.
    """

    shape: list[Behavior] = []

    for event in history.events:
        fields = event.fields

        if event.kind == "operation":
            shape.append(("op", fields.get("op"), fields.get("outcome")))

        elif event.kind == "trace":
            shape.append(
                (
                    "edge",
                    fields.get("trace_domain"),
                    fields.get("surface"),
                    fields.get("route"),
                    fields.get("op"),
                    fields.get("phase"),
                    fields.get("outcome"),
                )
            )

    return hashlib.blake2b(repr(shape).encode("utf-8"), digest_size=16).hexdigest()


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class CoverageStats:
    """The outcome of a coverage-guided sweep: how much behavior was exercised, and by whom."""

    behaviors: frozenset[Behavior]
    """Every distinct behavior seen across the swept seeds."""

    seeds_run: int
    """How many seeds actually ran (≤ the configured pool when the sweep stopped early)."""

    new_by_seed: tuple[tuple[int, int], ...]
    """Per seed, in run order: ``(seed, count of behaviors it added that no earlier seed had)``."""

    plateaued: bool
    """Whether the sweep stopped early because *behaviour* coverage saturated (vs exhausting the
    pool). Saturation on that alphabet is not saturation of the state space: measured on the misuse
    corpus, the plateau fires while only 14–37% of the reachable execution *shapes* have appeared.
    :attr:`deficit` is what puts a number on the rest."""

    shape_counts: Mapping[str, int] = NO_SHAPES
    """Execution-shape fingerprint → how many swept seeds produced it. Kept (rather than collapsed
    to a set) because the deficit estimator runs entirely on the frequency-of-frequencies —
    features seen exactly once, and exactly twice."""

    violation: ViolationReport | None = None
    """The first violating seed's minimized report, if the sweep hit one (it stops there)."""

    reachability: ReachabilityReport | None = None
    """Cross-sweep reachability outcome, when ``config.reachability_targets`` was declared: which
    "sometimes" states actually fired across the swept seeds (``None`` when none were declared).
    A non-empty :attr:`~forze_dst.oracle.reachability.ReachabilityReport.unreached` is false confidence —
    a target the sweep never drove."""

    confidence: ConfidenceReport | None = None
    """What the sweep actually exercised — operations that never raced, declared faults that never
    fired. Makes a green result honest: a clean sweep that left gaps here passed only what it
    tested. ``None`` when not computed (a bare ``coverage`` call still fills it)."""

    # ....................... #

    @property
    def size(self) -> int:
        """The number of distinct behaviors covered."""

        return len(self.behaviors)

    # ....................... #

    @cached_property
    def productive_seeds(self) -> tuple[int, ...]:
        """The seeds that each added at least one new behavior — the ones worth keeping."""

        return tuple(seed for seed, added in self.new_by_seed if added > 0)

    # ....................... #

    @cached_property
    def deficit(self) -> CoverageDeficit | None:
        """How much of the execution-shape alphabet the sweep did *not* reach (``None`` before any
        shapes were recorded).

        The measured counterweight to :attr:`plateaued`: a boolean saturation flag over the
        behaviour alphabet carries no uncertainty at all, and at the shipped ``coverage_plateau=8``
        a sweep still discovering on 10% of its seeds declares saturation 43% of the time.
        """

        return coverage_deficit(self.shape_counts) if self.shape_counts else None

    # ....................... #

    def format(self) -> str:
        """Render a short human summary of the exploration."""

        lines = [
            "DST coverage report",
            f"  behaviors covered: {self.size}",
            f"  seeds run:         {self.seeds_run}"
            + ("  (saturated on behaviours — plateau stop)" if self.plateaued else ""),
            f"  productive seeds:  {list(self.productive_seeds)}",
        ]

        # The saturation flag reports the behaviour alphabet; the deficit reports the one the
        # sweep did not measure. Printing both together is the point — on the corpus they
        # routinely disagree by 3–7×.
        deficit = self.deficit
        if deficit is not None:
            lines.append(f"  execution shapes:  {format_coverage_deficit(deficit)}")

        if self.reachability is not None:
            reached = len(self.reachability.targets) - len(self.reachability.unreached)
            lines.append(
                f"  reachability:      {reached}/{len(self.reachability.targets)} targets"
                + (
                    f"  (never reached: {sorted(self.reachability.unreached)})"
                    if self.reachability.unreached
                    else ""
                )
            )

        if self.confidence is not None and self.confidence.warnings:
            lines.append("  ⚠ confidence gaps (a clean sweep still left these untested):")
            lines.extend(f"      • {warning}" for warning in self.confidence.warnings)

        if self.violation is not None:
            names = ", ".join(sorted({v.invariant for v in self.violation.violations}))
            lines.append(f"  ✗ violation at seed {self.violation.seed}: {names}")
        elif self.seeds_run > 0:
            # Route through the confidence report when present, so an accounting-scoped sweep
            # prints the countable oracle-set clause here too (one claim, every surface).
            # A plateau stop makes ``seeds_run`` a stopping time — a random variable read off the
            # very runs being summarized — so the exact bound's fixed-design assumption fails and
            # the number is withheld rather than widened (a wide bound still reads as a result).
            # Gated on this report's own ``plateaued``, not the confidence report's copy of it, so
            # a mis-wired report can never leak a bound through this surface.
            if self.plateaued:
                verdict = format_withheld_verdict(self.seeds_run, stop_reason="plateau stop")
            elif self.confidence is not None:
                verdict = self.confidence.verdict(self.seeds_run)
            else:
                verdict = format_clean_verdict(self.seeds_run)

            lines.append(f"  {'⚠' if self.plateaued else '✓'} {verdict}")

        return "\n".join(lines)
