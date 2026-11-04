"""Outcome classes, per-step latency summaries, and the knee of a ramp."""

from __future__ import annotations

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
"""Outcomes the stop rule counts as errors: 5xx, a 504 deadline, and transport failures."""

MISCONFIGURED_OUTCOMES = frozenset({"forbidden", "client_error"})
"""Outcomes that mark a step misconfigured past :data:`MISCONFIGURED_SHARE`."""

MISCONFIGURED_SHARE = 0.05
"""Share of calls answering 401/403 or another unexpected 4xx that marks a step misconfigured:
a mix that answers 403 quickly looks like excellent throughput and is not a measurement."""

KNEE_GAIN = 0.05
"""A closed-model step under this gain over the previous step's goodput has stopped rising."""

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
            self.sketch.observe(latency_ms)
            self.max_ms = max(self.max_ms, latency_ms)

    def latency(self) -> dict[str, float | None]:
        ms: dict[str, float | None] = {
            name: _round(self.sketch.quantile(q)) for name, q in _QUANTILES
        }
        ms["max"] = _round(self.max_ms) if self.sketch.count else None

        return ms


@attrs.define
class StepRecorder:
    """One step's measurements: latency per scenario and in total, and outcome counts."""

    total: _Series = attrs.field(factory=_Series)
    scenarios: dict[str, _Series] = attrs.field(factory=lambda: dict[str, _Series]())
    scheduled: int = 0
    behind: int = 0

    # ....................... #

    def record(self, scenario: str, outcome: str, latency_ms: float | None) -> None:
        """Count one call. *latency_ms* is ``None`` for a send refused before it left."""

        self.total.add(outcome, latency_ms)
        self.scenarios.setdefault(scenario, _Series()).add(outcome, latency_ms)

    def summary(self, measured_s: float) -> dict[str, Any]:
        """The step's numbers as the output records them."""

        calls = sum(self.total.outcomes.values())
        ok = self.total.outcomes["ok"]
        misconfigured = sum(self.total.outcomes[o] for o in MISCONFIGURED_OUTCOMES)

        return {
            "duration_s": round(measured_s, 3),
            "calls": calls,
            "goodput_rps": _rate(ok, measured_s),
            "answered_rps": _rate(calls - self.total.outcomes["transport"], measured_s),
            "ms": self.total.latency(),
            "outcomes": {name: self.total.outcomes[name] for name in OUTCOMES},
            "misconfigured": calls > 0 and misconfigured > MISCONFIGURED_SHARE * calls,
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
    Open model: the first rate whose p99 more than doubles over the previous rate's, or whose
    answered rate falls more than :data:`KNEE_GAIN` below the offered rate.

    A step whose client was saturated, or that was misconfigured, is never named; the search
    stops there, because nothing after it measures the server.

    :returns: ``{"knee": <row key> | None, "confirmed": bool, "note": str | None}``.
    """

    usable: list[Mapping[str, Any]] = []

    for row in rows:
        if row["client"]["saturated"] or row["misconfigured"]:
            reason = "client saturated" if row["client"]["saturated"] else "misconfigured"
            found = _search(usable, model=model)

            if found["knee"] is not None:
                return found

            return {"knee": None, "confirmed": False, "note": f"{reason} at {_key(row)}"}

        usable.append(row)

    return _search(usable, model=model)


def _search(rows: Sequence[Mapping[str, Any]], *, model: str) -> dict[str, Any]:
    for k in range(1, len(rows)):
        before, here = rows[k - 1], rows[k]

        if model == "open":
            if _open_knee(before, here):
                return {"knee": _key(here), "confirmed": True, "note": None}

            continue

        ceiling = before["goodput_rps"] * (1.0 + KNEE_GAIN)

        if here["goodput_rps"] >= ceiling:
            continue

        if k + 1 < len(rows):
            if rows[k + 1]["goodput_rps"] < ceiling:
                return {"knee": _key(here), "confirmed": True, "note": None}

            continue

        return {"knee": _key(here), "confirmed": False, "note": "last step, unconfirmed"}

    return {"knee": None, "confirmed": False, "note": "goodput rose at every step"}


def _open_knee(before: Mapping[str, Any], here: Mapping[str, Any]) -> bool:
    p99_before, p99_here = before["ms"]["p99"], here["ms"]["p99"]

    if p99_before is not None and p99_here is not None and p99_here > 2.0 * p99_before:
        return True

    # Answered, not goodput: a mix with expected 409s answers every arrival below the offer.
    return bool(here["answered_rps"] < here["rate"] * (1.0 - KNEE_GAIN))


def _key(row: Mapping[str, Any]) -> dict[str, Any]:
    return {k: row[k] for k in ("workers", "rate") if k in row}


def _rate(count: int, seconds: float) -> float:
    return round(count / seconds, 2) if seconds > 0 else 0.0


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 2)
