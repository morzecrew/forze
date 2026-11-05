"""JSON lines output, and the comparison of two runs.

A run file is one JSON object per line: a ``meta`` record, one ``step`` row per step, and a
``summary``. It holds counts, latencies and outcome classes only, never bodies, headers,
tokens or ids.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from types import TracebackType
from typing import IO, Any, Self

import attrs

from forze.base.exceptions import exc

from .stats import ERROR_OUTCOMES

# ----------------------- #

SCHEMA = 1
"""Version of the run file's shape, recorded in its ``meta`` line."""

_LOWER_IS_WORSE = frozenset({"goodput"})
_METRICS: tuple[str, ...] = ("goodput", "p50", "p95", "p99")
_PERCENTILES: tuple[str, ...] = ("p50", "p95", "p99")
_META_IGNORED = frozenset({"kind", "schema", "started_at"})
_META_LABELS: dict[str, str] = {
    "forze_load": "forze_load version",
    "client.cpus": "client cores",
    "client.python": "client Python",
    "client.loop": "client event loop",
    "plan.principals": "principal weights",
    "plan.mixes": "principal mixes",
}


# ....................... #


class RunWriter:
    """Writes a run's records as JSON lines, flushing each, or nothing for a ``None`` path."""

    def __init__(self, path: str | Path | None) -> None:
        self._path = Path(path) if path is not None else None
        self._file: IO[str] | None = None

    def __enter__(self) -> Self:
        if self._path is not None:
            self._file = self._path.open("w", encoding="utf-8")

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._file is not None:
            self._file.close()

    def write(self, record: Mapping[str, Any]) -> None:
        """Append one record as a line."""

        if self._file is not None:
            self._file.write(json.dumps(record, separators=(",", ":")) + "\n")
            self._file.flush()


# ....................... #


@attrs.frozen
class RunFile:
    """A run file read back: its meta record, rows and summary (``None`` if interrupted)."""

    meta: dict[str, Any]
    rows: list[dict[str, Any]]
    summary: dict[str, Any] | None


def read_run(path: str | Path) -> RunFile:
    """Read a run file written by :func:`forze_load.run`.

    :raises CoreException: ``configuration`` (``load_run_file_invalid``) — the file cannot
        be read, a line is not a JSON object, there is no ``meta`` line, the schema is one
        this version does not read, or a step row lacks its key, goodput or latency.
    """

    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
        records = [json.loads(line) for line in lines if line.strip()]

    except (OSError, ValueError) as error:
        raise _invalid(f"{path} cannot be read as a run file ({type(error).__name__})") from error

    if not all(isinstance(record, dict) for record in records):
        raise _invalid(f"{path} has a line that is not a JSON object")

    if not records or records[0].get("kind") != "meta":
        raise _invalid(f"{path} is not a run file")

    meta = records[0]

    if meta.get("schema") != SCHEMA:
        raise exc.configuration(
            f"{path} has schema {meta.get('schema')}, this version reads {SCHEMA}",
            code="load_run_file_invalid",
        )

    if not _is_plan(meta.get("plan", {})):
        raise _invalid(f"{path} has a meta plan whose weights or mixes are not objects")

    rows = [record for record in records if record.get("kind") == "step"]

    if not all(_is_step(row) for row in rows):
        raise _invalid(f"{path} has a step row without a numeric key, goodput or latency")

    summary = next((record for record in records if record.get("kind") == "summary"), None)

    return RunFile(meta=meta, rows=rows, summary=summary)


def _is_number(value: Any) -> bool:
    # Python's JSON reader accepts NaN and Infinity; either would make a threshold never fail.
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value)


def _is_step(row: Mapping[str, Any]) -> bool:
    """A step row ``compare`` can read: a finite key and goodput, and the compared percentiles
    present, each a finite number or null (a percentile with no answered call)."""

    key = row.get("workers", row.get("rate"))
    ms = row.get("ms")

    return (
        _is_number(key)
        and _is_number(row.get("goodput_rps"))
        and isinstance(ms, dict)
        and all(name in ms for name in _PERCENTILES)
        and all(value is None or _is_number(value) for value in ms.values())
    )


def _is_plan(plan: Any) -> bool:
    return isinstance(plan, dict) and all(
        isinstance(plan.get(key, {}), dict) for key in ("principals", "mixes")
    )


def _invalid(summary: str) -> Exception:
    return exc.configuration(summary, code="load_run_file_invalid")


# ....................... #


@attrs.frozen
class Comparison:
    """Two runs side by side: a line per matched step, warnings, and threshold failures."""

    lines: list[str]
    warnings: list[str]
    failures: list[str]


def parse_fail_on(spec: str) -> dict[str, float]:
    """Parse ``"p95:15%,goodput:10%"`` into ``{"p95": 0.15, "goodput": 0.10}``.

    :raises CoreException: ``configuration`` (``load_fail_on_invalid``) — an unknown metric
        or a threshold that is not a positive percentage.
    """

    thresholds: dict[str, float] = {}

    for part in (item.strip() for item in spec.split(",")):
        metric, _, value = part.partition(":")

        try:
            share = float(value.strip().removesuffix("%")) / 100.0

        except ValueError:
            share = 0.0

        # `nan > limit` is always false: a nan threshold would never fail anything.
        if metric not in _METRICS or not (math.isfinite(share) and share > 0):
            raise exc.configuration(
                f"--fail-on takes metric:percent with a metric in {list(_METRICS)}, got {part!r}",
                code="load_fail_on_invalid",
            )

        thresholds[metric] = share

    return thresholds


def compare_runs(
    base: RunFile,
    head: RunFile,
    *,
    fail_on: Mapping[str, float] | None = None,
) -> Comparison:
    """Compare *head* against *base*, step by step.

    Rows are matched by worker count or rate. Goodput is worse when it falls, latency when
    it rises. Without *fail_on* nothing fails: load runs vary far more than micro-benchmarks,
    so a default threshold would make a gate flaky.
    """

    # Another host, runtime or mix is another measurement, not a regression: say which.
    warnings = [
        f"runs differ in {_META_LABELS.get(name, name.replace('.', ' '))}: comparing anyway"
        for name in _meta_differences(base.meta, head.meta)
    ]
    base_rows, head_rows = _keyed(base.rows), _keyed(head.rows)
    lines: list[str] = []
    failures: list[str] = []

    for key in [k for k in base_rows if k in head_rows]:
        cells: list[str] = []
        label = _label(key)

        for metric in _METRICS:
            old, new = _metric(base_rows[key], metric), _metric(head_rows[key], metric)
            change = _change(old, new, metric)
            cells.append(f"{metric} {_fmt(old)} -> {_fmt(new)}{_fmt_change(change)}")
            limit = (fail_on or {}).get(metric)

            if limit is None:
                continue

            # A latency the head run lost (no call answered) is not "unchanged": it fails.
            if old is not None and new is None:
                failures.append(f"{label}: {metric} has no value in head (base {_fmt(old)})")

            elif change is not None and change > limit:
                failures.append(f"{label}: {metric} worse by {change:.1%} (limit {limit:.0%})")

        lines.append(f"{label}: " + ", ".join(cells))

    missing = sorted(set(base_rows) ^ set(head_rows))
    warnings.extend(f"{_label(key)}: only in one run" for key in missing)

    if not lines:
        # A gate that compared nothing must not pass: a run cut short before its first step
        # writes only its meta line.
        if fail_on:
            raise exc.configuration(
                "--fail-on needs at least one step present in both runs, and none matched",
                code="load_compare_empty",
            )

        warnings.append("no step matched between the runs: nothing was compared")

    return Comparison(lines=lines, warnings=warnings, failures=failures)


# ....................... #


def _meta_differences(base: Mapping[str, Any], head: Mapping[str, Any]) -> list[str]:
    """Every meta field the two runs disagree on but ``started_at``, nested one level."""

    names: list[str] = []

    for key in [*base, *(k for k in head if k not in base)]:
        if key in _META_IGNORED:
            continue

        old, new = base.get(key), head.get(key)

        if isinstance(old, dict) and isinstance(new, dict):
            names.extend(
                f"{key}.{sub}"
                for sub in [*old, *(k for k in new if k not in old)]
                if old.get(sub) != new.get(sub)
            )

        elif old != new:
            names.append(key)

    return names


type _StepKey = tuple[str, float, int]


def _step(row: Mapping[str, Any]) -> tuple[str, float]:
    """A step's key as a number, so ``rate=50`` and ``rate=50.0`` are one step."""

    return ("workers", float(row["workers"])) if "workers" in row else ("rate", float(row["rate"]))


def _keyed(rows: Sequence[Mapping[str, Any]]) -> dict[_StepKey, Mapping[str, Any]]:
    """Rows by step and occurrence: a ramp that repeats a step compares each by position."""

    seen: Counter[tuple[str, float]] = Counter()
    keyed: dict[_StepKey, Mapping[str, Any]] = {}

    for row in rows:
        step = _step(row)
        seen[step] += 1
        keyed[(*step, seen[step])] = row

    return keyed


def _label(key: _StepKey) -> str:
    kind, value, occurrence = key
    label = f"{kind}={value:g}"

    if occurrence == 1:
        return label

    return f"{label} ({ {2: '2nd', 3: '3rd'}.get(occurrence, f'{occurrence}th') })"


def _row_key(row: Mapping[str, Any]) -> str:
    kind, value = _step(row)

    return f"{kind}={value:g}"


def _metric(row: Mapping[str, Any], metric: str) -> float | None:
    value = row["goodput_rps"] if metric == "goodput" else row["ms"].get(metric)

    return None if value is None else float(value)


def _change(old: float | None, new: float | None, metric: str) -> float | None:
    """How much worse *new* is than *old*, as a share; negative is better."""

    if old is None or new is None or old == 0:
        return None

    worse = (old - new) if metric in _LOWER_IS_WORSE else (new - old)

    return worse / old


def _fmt(value: float | None) -> str:
    return "-" if value is None else f"{value:g}"


def _fmt_change(change: float | None) -> str:
    return "" if change is None else f" ({'worse' if change > 0 else 'better'} {abs(change):.1%})"


def summarize(rows: Sequence[Mapping[str, Any]], summary: Mapping[str, Any]) -> list[str]:
    """Human-readable lines for a run, one per step and one for the summary."""

    lines = [
        f"{_row_key(row)}: goodput {row['goodput_rps']}/s, p95 {_fmt(row['ms']['p95'])} ms, "
        f"p99 {_fmt(row['ms']['p99'])} ms, errors "
        f"{sum(row['outcomes'].get(name, 0) for name in ERROR_OUTCOMES)}"
        + (", client saturated" if row["client"]["saturated"] else "")
        + (", misconfigured" if row["misconfigured"] else "")
        for row in rows
    ]
    knee = summary.get("knee")
    note = summary.get("note")
    lines.append(f"knee: {knee if knee is not None else 'none'}" + (f" ({note})" if note else ""))

    if summary.get("stopped"):
        lines.append(f"stopped: {summary['stopped']}")

    return lines
