"""Outcome classes, per-step latency summaries, and the knee of a ramp."""

from __future__ import annotations

import asyncio
import contextlib
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

import attrs

from forze.base.primitives import DDSketch

# ----------------------- #

OUTCOMES: tuple[str, ...] = (
    "ok",
    "conflict",
    "shed",
    "deadline",
    "forbidden",
    "not_found",
    "client_error",
    "server_error",
    "transport",
)
"""Every outcome class, in output order. Goodput counts ``ok`` only."""

ERROR_OUTCOMES = frozenset({"server_error", "deadline", "transport"})
"""Outcomes the stop rule counts as errors: 5xx, a 504 deadline, and transport failures
(a send refused at the open model's in-flight cap included)."""

EXPECTED_OUTCOMES = frozenset({"conflict", "not_found"})
"""Answers a healthy server gives a write or id mix; the open-model knee discounts them."""

MISCONFIGURED_OUTCOMES = frozenset({"forbidden", "client_error"})
"""Outcomes that mark a step misconfigured past :data:`MISCONFIGURED_SHARE`."""

MISCONFIGURED_SHARE = 0.05
"""Share of calls answering 401/403 or another unexpected 4xx that marks a step misconfigured:
a mix that answers 403 quickly looks like excellent throughput and is not a measurement."""

KNEE_GAIN = 0.05
"""A closed-model step under this gain over the previous step's goodput has stopped rising."""

KNEE_MIN_SAMPLES = 100
"""Answered calls a step needs before its p99 is compared: below 100, p99 is the maximum."""

_QUANTILES: tuple[tuple[str, float], ...] = (
    ("p50", 0.5),
    ("p90", 0.9),
    ("p95", 0.95),
    ("p99", 0.99),
    ("p999", 0.999),
)


# ....................... #


def classify(status: int) -> str:
    """The outcome class of an HTTP status."""

    if 200 <= status < 300:
        return "ok"

    match status:
        case 409:
            return "conflict"
        case 429:
            return "shed"
        case 504:
            return "deadline"
        case 401 | 403:
            return "forbidden"
        case 404:
            return "not_found"
        case _:
            return "server_error" if status >= 500 else "client_error"


# ....................... #


@attrs.define
class _Series:
    sketch: DDSketch = attrs.field(factory=DDSketch)
    outcomes: Counter[str] = attrs.field(factory=lambda: Counter[str]())
    max_ms: float = 0.0

    def add(self, outcome: str, latency_ms: float | None) -> None:
        self.outcomes[outcome] += 1

        if latency_ms is not None:
            self.sketch.observe(max(latency_ms, 0.0))
            self.max_ms = max(self.max_ms, latency_ms)

    def latency(self) -> dict[str, float | None]:
        ms: dict[str, float | None] = {
            name: _round(self.sketch.quantile(q)) for name, q in _QUANTILES
        }
        ms["max"] = _round(self.max_ms) if self.sketch.count else None

        return ms


@attrs.define
class StepRecorder:
    """One step's measurements, bound to each call when it is sent.

    A call sent during the step's measured window records here even when it answers after
    the step ended, so a slow answer counts in the step that sent it. :meth:`drain` waits
    for those calls before the row is written.
    """

    warm_until: float
    """``perf_counter`` time when the step's measured window opens."""

    total: _Series = attrs.field(factory=_Series)
    scenarios: dict[str, _Series] = attrs.field(factory=lambda: dict[str, _Series]())
    scheduled: int = 0
    behind: int = 0
    auth_s: float = 0.0
    auth_max_s: float = 0.0
    inflight: int = 0
    _closed: bool = False
    _drained: asyncio.Event = attrs.field(factory=asyncio.Event)

    # ....................... #

    def enter(self) -> None:
        """A measured call was sent."""

        self.inflight += 1

    def leave(self) -> None:
        """A measured call finished."""

        self.inflight -= 1

        if self._closed and self.inflight == 0:
            self._drained.set()

    async def drain(self, timeout: float) -> None:
        """Wait up to *timeout* seconds for the calls this step sent to finish."""

        self._closed = True

        if self.inflight == 0:
            return

        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._drained.wait(), timeout=timeout)

    def record(self, scenario: str, outcome: str, latency_ms: float | None) -> None:
        """Count one call. *latency_ms* is ``None`` for a call that got no answer."""

        self.total.add(outcome, latency_ms)
        self.scenarios.setdefault(scenario, _Series()).add(outcome, latency_ms)

    def add_auth(self, seconds: float) -> None:
        """Count time spent resolving a call's credential, kept out of its latency."""

        self.auth_s += seconds
        self.auth_max_s = max(self.auth_max_s, seconds)

    def summary(self, measured_s: float) -> dict[str, Any]:
        """The step's numbers as the output records them, over *measured_s* seconds."""

        calls = sum(self.total.outcomes.values())
        ok = self.total.outcomes["ok"]
        misconfigured = sum(self.total.outcomes[o] for o in MISCONFIGURED_OUTCOMES)

        return {
            "duration_s": round(measured_s, 3),
            "calls": calls,
            "goodput_rps": round(ok / measured_s, 2) if measured_s > 0 else 0.0,
            "sampled": self.total.sketch.count,
            "ms": self.total.latency(),
            "outcomes": {name: self.total.outcomes[name] for name in OUTCOMES},
            "misconfigured": calls > 0 and misconfigured > MISCONFIGURED_SHARE * calls,
            "unfinished": self.inflight,
            "auth": {"total_s": round(self.auth_s, 3), "max_ms": round(self.auth_max_s * 1000, 2)},
            "scenarios": {
                name: {
                    "calls": sum(series.outcomes.values()),
                    "ok": series.outcomes["ok"],
                    "ms": series.latency(),
                }
                for name, series in sorted(self.scenarios.items())
            },
        }


def error_share(row: Mapping[str, Any]) -> float:
    """The share of a step's calls the stop rule counts as errors."""

    calls = row["calls"]

    if not calls:
        return 0.0

    return sum(row["outcomes"][name] for name in ERROR_OUTCOMES) / calls


# ....................... #


def find_knee(rows: Sequence[Mapping[str, Any]], *, model: str) -> dict[str, Any]:
    """Name the step where goodput stops rising, or say why none is named.

    Closed model: the first step whose goodput is under :data:`KNEE_GAIN` above the previous
    step's, confirmed by the next step (also under the gain over the same previous step).
    Open model: the first rate whose p99 more than doubles over the previous rate's, each
    with at least :data:`KNEE_MIN_SAMPLES` answered calls; or whose goodput falls more than
    :data:`KNEE_GAIN` below the offered rate less its ``conflict`` and ``not_found``
    answers. Either is confirmed only when the next rate repeats it.

    A step whose client was saturated, or that was misconfigured, is never named and never
    confirms a knee; the search stops there, because nothing after it measures the server.

    :returns: ``{"knee": <row key> | None, "confirmed": bool, "note": str | None}``.
    """

    usable: list[Mapping[str, Any]] = []
    blocker: str | None = None

    for row in rows:
        if row["client"]["saturated"] or row["misconfigured"]:
            reason = "client saturated" if row["client"]["saturated"] else "misconfigured"
            blocker = f"{reason} at {_key(row)}"
            break

        usable.append(row)

    found = _search(usable, model=model)

    if blocker is None:
        return found

    if found["knee"] is None:
        return {"knee": None, "confirmed": False, "note": blocker}

    if not found["confirmed"]:
        return {**found, "note": f"unconfirmed: the next step was {blocker}"}

    return found


def _search(rows: Sequence[Mapping[str, Any]], *, model: str) -> dict[str, Any]:
    if model == "open":
        return _open_search(rows)

    if len(rows) < 2:
        note = "one step, no comparison" if rows else "no step measured the server"
        return {"knee": None, "confirmed": False, "note": note}

    for k in range(1, len(rows)):
        before, here = rows[k - 1], rows[k]
        after = rows[k + 1] if k + 1 < len(rows) else None

        if before["goodput_rps"] <= 0:
            return {"knee": None, "confirmed": False, "note": f"no goodput at {_key(before)}"}

        ceiling = before["goodput_rps"] * (1.0 + KNEE_GAIN)

        if here["goodput_rps"] >= ceiling:
            continue

        if after is None:
            return {"knee": _key(here), "confirmed": False, "note": "last step, unconfirmed"}

        if after["goodput_rps"] < ceiling:
            return {"knee": _key(here), "confirmed": True, "note": None}

    return {"knee": None, "confirmed": False, "note": "goodput rose at every step"}


def _open_search(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    # From the first rate: a shortfall compares a step with its own offer, so the first rate
    # can be the knee. Only the p99 test needs an earlier rate.
    for k, here in enumerate(rows):
        before = rows[k - 1] if k else None
        after = rows[k + 1] if k + 1 < len(rows) else None
        found = _open_knee(before, here, after)

        if found is not None:
            return found

    if len(rows) < 2:
        note = "one step, no comparison" if rows else "no step measured the server"
        return {"knee": None, "confirmed": False, "note": note}

    return {"knee": None, "confirmed": False, "note": "goodput kept pace at every rate"}


def _open_knee(
    before: Mapping[str, Any] | None,
    here: Mapping[str, Any],
    after: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    # Each test needs the next rate to repeat it, as the closed model's does: one step that
    # falls short or spikes can be a transient, not a capacity limit.
    def spiked(row: Mapping[str, Any]) -> bool:
        return before is not None and _p99_doubled(before, row)

    for holds in (_short, spiked):
        if not holds(here):
            continue

        if after is None:
            return {"knee": _key(here), "confirmed": False, "note": "last step, unconfirmed"}

        if holds(after):
            return {"knee": _key(here), "confirmed": True, "note": None}

    return None


def _short(row: Mapping[str, Any]) -> bool:
    """Goodput more than :data:`KNEE_GAIN` below what the offered rate should yield."""

    return bool(row["goodput_rps"] < _expected_ok(row) * (1.0 - KNEE_GAIN))


def _expected_ok(row: Mapping[str, Any]) -> float:
    """The offered rate less the answers a healthy server gives that are not 2xx."""

    seconds = row["duration_s"]
    expected = sum(row["outcomes"][name] for name in EXPECTED_OUTCOMES)

    return float(row["rate"]) - (expected / seconds if seconds > 0 else 0.0)


def _p99_doubled(before: Mapping[str, Any], later: Mapping[str, Any]) -> bool:
    if min(before["sampled"], later["sampled"]) < KNEE_MIN_SAMPLES:
        return False

    p99_before, p99_later = before["ms"]["p99"], later["ms"]["p99"]

    return p99_before is not None and p99_later is not None and p99_later > 2.0 * p99_before


def _key(row: Mapping[str, Any]) -> dict[str, Any]:
    return {k: row[k] for k in ("workers", "rate") if k in row}


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 2)
