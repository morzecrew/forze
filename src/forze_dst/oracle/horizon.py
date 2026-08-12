"""Horizon analysis — could the simulation ever catch each declared invariant failing?

A clean sweep only speaks about invariants that were *at risk*. This module supplies the two
mechanical flags that catch the cheapest overclaims with no perturbation at all:

* **vacuity** — an invariant whose read footprint (the event kinds its predicate actually
  queries, measured by running it once against a footprint-tracing probe) never intersects the
  kinds any run recorded was never at risk; its green is a constant, not a result.
* **marker-blindness** — an invariant folding markers that handler code emitted *inside a
  transaction scope that later rolled back* inherits the rollback blind spot: the marker
  survives in history while the port effects it claims to witness do not. The corpus authoring
  rule ("oracles read port state, never markers") exists for exactly this hazard.

The footprint probe is dynamic on purpose: invariant predicates are opaque closures over
:class:`~forze_dst.oracle.recorder.History`, so the practical static analysis is to run them once
and record what they read. An invariant that touches ``history.events`` wholesale has an unknown
footprint and is never flagged (conservative). Both flags are warnings feeding the confidence
report — they name a hazard to review, not a proven defect.

The same footprint relation, applied **per run** rather than sweep-wide, is also what makes the
clean-run verdict's denominator honest. Vacuity is only its binary edge — at risk in ≥1 run, or in
none. The counts in between are what a single sweep-wide ``S`` overstates: an invariant exposed on
50 of 1000 runs is covered by a sentence quoting the bound for 1000, which is 20× stronger than
its own evidence supports.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from contextlib import suppress
from typing import cast, final

import attrs

from forze_dst.oracle.invariants import Invariant, name_of
from forze_dst.oracle.recorder import Event, History

# ----------------------- #

MACHINERY_KINDS = frozenset(
    {
        "trace",
        "operation",
        "op_start",
        "fault",
        "partition",
        "deadlock",
        "crash",
        "node_error",
        "reached",
        "phase",
    }
)
"""Event kinds the simulation machinery records itself. Everything else is an app *marker*
(``record_event`` from handler or observe code) — the population the marker-blindness flag
classifies."""


# ....................... #


class _FootprintProbe:
    """A duck-typed :class:`History` that records which kinds an invariant reads.

    ``of_kind`` notes the kind and returns no events (no predicate ever runs, so probing can
    never crash on synthetic data); touching ``events`` wholesale marks the footprint opaque.
    """

    seed = 0

    def __init__(self) -> None:
        self.kinds: set[str] = set()
        self.opaque = False

    @property
    def events(self) -> tuple[Event, ...]:
        self.opaque = True
        return ()

    def of_kind(self, kind: str) -> list[Event]:
        self.kinds.add(kind)
        return []


# ....................... #


def read_kinds(invariant: Invariant) -> frozenset[str] | None:
    """The event kinds *invariant* reads, or ``None`` when its footprint is unknowable.

    Unknowable = it iterates ``history.events`` directly, or it read nothing before returning
    or raising — either way vacuity cannot be decided, so the caller must not flag it.

    One more scope limit: the probe's ``of_kind`` always returns an empty list, so a predicate
    that early-returns on an empty first read never reaches its *later* ``of_kind`` calls — the
    footprint then omits those gated kinds. That direction is safe (a smaller footprint can only
    make the vacuity/marker flags *more* conservative, never a false positive), but the reported
    kinds are the empty-history read path, not every kind the invariant could ever touch.
    """

    probe = _FootprintProbe()

    # What the invariant read before raising still counts; its verdict is discarded either way.
    with suppress(Exception):
        invariant(cast("History", probe))

    if probe.opaque or not probe.kinds:
        return None

    return frozenset(probe.kinds)


# ....................... #


@final
@attrs.frozen(kw_only=True)
class HorizonAnalysis:
    """The per-sweep horizon flags, keyed by declared invariant name."""

    vacuous: tuple[tuple[str, tuple[str, ...]], ...] = ()
    """``(invariant, read kinds)`` pairs where no run ever recorded any kind the invariant
    reads — the invariant was never at risk, its green is a constant."""

    marker_blind: tuple[tuple[str, tuple[str, ...]], ...] = ()
    """``(invariant, blind kinds)`` pairs where the invariant folds handler-emitted markers
    recorded while a transaction scope that later **rolled back** was in flight."""

    runs: int = 0
    """How many runs were folded — the denominator every :attr:`at_risk` count is out of."""

    at_risk: tuple[tuple[str, int], ...] = ()
    """``(invariant, runs whose recorded kinds intersected its read footprint)``. Vacuity is the
    binary edge of this measurement (a count of zero); the counts in between are what a single
    sweep-wide ``S`` silently overstates — an invariant exposed on 50 of 1000 runs is covered by a
    verdict quoting the bound for 1000."""

    unmeasured_exposure: tuple[str, ...] = ()
    """Invariants whose read footprint is opaque (they iterate ``history.events`` wholesale), so
    how often they were at risk cannot be measured. Reported as such, never folded in at
    ``n = runs`` — the existing conservative posture on opaque footprints, extended."""


# ....................... #


def _observe_boundary(history: History) -> int | None:
    """The seq of the run's ``observe`` phase boundary, or ``None`` when no observe hook ran."""

    return next(
        (
            event.seq
            for event in history.events
            if event.kind == "phase" and event.fields.get("phase") == "observe"
        ),
        None,
    )


def _rolled_back_windows(history: History) -> list[tuple[float, float]]:
    """The ``[enter, exit]`` virtual-time windows of every transaction that rolled back."""

    windows: list[tuple[float, float]] = []
    entered: dict[object, float] = {}

    for event in history.of_kind("trace"):
        fields = event.fields
        if fields.get("trace_domain") != "tx":
            continue

        tx_id = fields.get("tx_id")
        if fields.get("op") == "enter" and tx_id is not None:
            entered[tx_id] = event.at
        elif (
            fields.get("op") == "exit" and fields.get("outcome") == "rollback" and tx_id in entered
        ):
            windows.append((entered[tx_id], event.at))

    return windows


# ....................... #


@attrs.define
class HorizonProbe:
    """Folds per-run histories into the horizon signals, one history at a time (incremental,
    like the confidence probe — a sweep never holds every history in memory).

    Pass *invariants* to also count, **per run**, which of them were actually at risk. Without it
    the probe still flags vacuity and marker-blindness from the sweep-wide union of kinds, but
    every invariant's exposure is reported as unmeasured rather than assumed to be the full sweep.
    """

    invariants: Sequence[Invariant] = ()

    _present: set[str] = attrs.field(factory=set)
    _blind_kinds: set[str] = attrs.field(factory=set)
    _footprints: dict[str, frozenset[str] | None] = attrs.field(factory=dict)
    _at_risk: Counter[str] = attrs.field(factory=Counter)
    _runs: int = 0

    # ....................... #

    def __attrs_post_init__(self) -> None:
        """Resolve each declared invariant's read footprint once, before any run is folded.

        The footprint probe *runs the predicate* (against an empty synthetic history), so it is
        resolved exactly once per invariant and reused by both ``observe`` and ``analyze`` —
        neither the per-run fold nor the final analysis probes a predicate again. Doing it here
        also means the probe is ready to count from the very first ``observe``. A ``None`` entry
        records an opaque footprint, which is a different fact from "not declared". Repeated names
        collapse to one entry with the union of their footprints, so a run can never be counted
        twice for one name — the accounting gate is what forbids duplicate names in the first
        place.
        """

        for invariant in self.invariants:
            kinds = read_kinds(invariant)
            name = name_of(invariant)
            known = self._footprints.get(name)

            if kinds is None or (name in self._footprints and known is None):
                self._footprints[name] = None  # opaque wins: an unknown footprint stays unknown
            else:
                self._footprints[name] = (known or frozenset()) | kinds

    # ....................... #

    def _footprint_of(self, invariant: Invariant, name: str) -> frozenset[str] | None:
        """The read footprint of *invariant*, from the resolved table when it was declared here."""

        if name in self._footprints:
            return self._footprints[name]

        return read_kinds(invariant)

    # ....................... #

    def observe(self, history: History) -> None:
        """Fold one recorded run: which kinds exist, which markers overlap a rollback, and which
        declared invariants this run actually put at risk.

        Handler-emitted = recorded before the ``observe`` phase boundary (markers from the
        ``observe`` hook run after the workload over settled state and carry no rollback
        hazard). Rollback overlap is by virtual-time window ``[enter, exit]`` of a rolled-back
        transaction — under concurrency a same-window marker from another task can be swept in,
        so a hit is a hazard to review, not a proven blind oracle.
        """

        boundary = _observe_boundary(history)
        rolled_back = _rolled_back_windows(history)
        kinds_here: set[str] = set()
        self._runs += 1

        for event in history.events:
            self._present.add(event.kind)
            kinds_here.add(event.kind)

            if event.kind in MACHINERY_KINDS:
                continue
            if boundary is not None and event.seq >= boundary:
                continue  # observe-emitted: recorded over settled state, no rollback hazard

            if any(start <= event.at <= end for start, end in rolled_back):
                self._blind_kinds.add(event.kind)

        # "At risk" is the same relation vacuity uses, applied per run rather than sweep-wide:
        # this run recorded at least one kind the invariant's predicate reads. An opaque footprint
        # (``None``) is skipped so the counter never holds a number nothing could justify —
        # ``analyze`` is the load-bearing exclusion (it reports those names as unmeasured and
        # never reads their count), this keeps the accumulator itself honest.
        for name, footprint in self._footprints.items():
            if footprint is not None and footprint & kinds_here:
                self._at_risk[name] += 1

    # ....................... #

    def analyze(self, invariants: Sequence[Invariant]) -> HorizonAnalysis:
        """Flag each invariant against everything folded so far."""

        vacuous: list[tuple[str, tuple[str, ...]]] = []
        blind: list[tuple[str, tuple[str, ...]]] = []
        at_risk: list[tuple[str, int]] = []
        unmeasured: list[str] = []

        for invariant in invariants:
            name = name_of(invariant)
            kinds = self._footprint_of(invariant, name)

            if kinds is None:
                # Opaque footprint — vacuity cannot be decided, and neither can exposure. Named
                # rather than folded into the aggregate at n = runs, which would be the overclaim
                # this measurement exists to remove.
                unmeasured.append(name)
                continue

            if name in self._footprints:
                at_risk.append((name, self._at_risk[name]))
            else:
                # Counted footprints come from the invariants the probe was *constructed* with;
                # anything analyzed but not declared there was never measured per run.
                unmeasured.append(name)

            if not kinds & self._present:
                vacuous.append((name, tuple(sorted(kinds))))

            folded_blind = kinds & self._blind_kinds
            if folded_blind:
                blind.append((name, tuple(sorted(folded_blind))))

        return HorizonAnalysis(
            vacuous=tuple(vacuous),
            marker_blind=tuple(blind),
            runs=self._runs,
            at_risk=tuple(at_risk),
            unmeasured_exposure=tuple(unmeasured),
        )
