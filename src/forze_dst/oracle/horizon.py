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
"""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, cast, final

import attrs

from forze_dst.oracle.invariants import Invariant, name_of
from forze_dst.oracle.recorder import Event, History

if TYPE_CHECKING:
    from collections.abc import Sequence

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
    like the confidence probe — a sweep never holds every history in memory)."""

    _present: set[str] = attrs.field(factory=set)
    _blind_kinds: set[str] = attrs.field(factory=set)

    # ....................... #

    def observe(self, history: History) -> None:
        """Fold one recorded run: which kinds exist, and which markers overlap a rollback.

        Handler-emitted = recorded before the ``observe`` phase boundary (markers from the
        ``observe`` hook run after the workload over settled state and carry no rollback
        hazard). Rollback overlap is by virtual-time window ``[enter, exit]`` of a rolled-back
        transaction — under concurrency a same-window marker from another task can be swept in,
        so a hit is a hazard to review, not a proven blind oracle.
        """

        boundary = _observe_boundary(history)
        rolled_back = _rolled_back_windows(history)

        for event in history.events:
            self._present.add(event.kind)

            if event.kind in MACHINERY_KINDS:
                continue
            if boundary is not None and event.seq >= boundary:
                continue  # observe-emitted: recorded over settled state, no rollback hazard

            if any(start <= event.at <= end for start, end in rolled_back):
                self._blind_kinds.add(event.kind)

    # ....................... #

    def analyze(self, invariants: Sequence[Invariant]) -> HorizonAnalysis:
        """Flag each invariant against everything folded so far."""

        vacuous: list[tuple[str, tuple[str, ...]]] = []
        blind: list[tuple[str, tuple[str, ...]]] = []

        for invariant in invariants:
            kinds = read_kinds(invariant)
            if kinds is None:
                continue  # opaque footprint — cannot be decided, so never flagged

            name = name_of(invariant)

            if not kinds & self._present:
                vacuous.append((name, tuple(sorted(kinds))))

            folded_blind = kinds & self._blind_kinds
            if folded_blind:
                blind.append((name, tuple(sorted(folded_blind))))

        return HorizonAnalysis(vacuous=tuple(vacuous), marker_blind=tuple(blind))
