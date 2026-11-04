"""JSON lines output, and the comparison of two runs.

A run file is one JSON object per line: a ``meta`` record, one ``step`` row per step, and a
``summary``. It holds counts, latencies and outcome classes only, never bodies, headers,
tokens or ids.
"""

from __future__ import annotations

import json
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
_META_KEYS: tuple[str, ...] = ("target", "model", "client")


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

    rows = [record for record in records if record.get("kind") == "step"]

    if not all(_is_step(row) for row in rows):
        raise _invalid(f"{path} has a step row without its key, goodput or latency")

    summary = next((record for record in records if record.get("kind") == "summary"), None)

    return RunFile(meta=meta, rows=rows, summary=summary)


def _is_step(row: Mapping[str, Any]) -> bool:
    return (
        ("workers" in row or "rate" in row)
        and isinstance(row.get("goodput_rps"), int | float)
        and isinstance(row.get("ms"), dict)
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

        if metric not in _METRICS or share <= 0:
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

    warnings = [
        f"runs differ in {key}: comparing anyway"
        for key in _META_KEYS
        if base.meta.get(key) != head.meta.get(key)
    ]
    base_plan, head_plan = base.meta.get("plan") or {}, head.meta.get("plan") or {}
    warnings.extend(
        f"runs differ in principal {label}: a different mix is a different workload"
        for key, label in (("principals", "weights"), ("mixes", "mixes"))
        if base_plan.get(key) != head_plan.get(key)
    )
    other = ("principals", "mixes")

    if {k: v for k, v in base_plan.items() if k not in other} != {
        k: v for k, v in head_plan.items() if k not in other
    }:
        warnings.append("runs differ in plan (scenarios, timeout or seed): comparing anyway")
    base_rows = {_row_key(row): row for row in base.rows}
    head_rows = {_row_key(row): row for row in head.rows}
    lines: list[str] = []
    failures: list[str] = []

    for key in [k for k in base_rows if k in head_rows]:
        cells: list[str] = []

        for metric in _METRICS:
            old, new = _metric(base_rows[key], metric), _metric(head_rows[key], metric)
            change = _change(old, new, metric)
            cells.append(f"{metric} {_fmt(old)} -> {_fmt(new)}{_fmt_change(change)}")
            limit = (fail_on or {}).get(metric)

            if limit is not None and change is not None and change > limit:
                failures.append(f"{key}: {metric} worse by {change:.1%} (limit {limit:.0%})")

        lines.append(f"{key}: " + ", ".join(cells))

    missing = sorted(set(base_rows) ^ set(head_rows))
    warnings.extend(f"{key}: only in one run" for key in missing)

    return Comparison(lines=lines, warnings=warnings, failures=failures)


# ....................... #


def _row_key(row: Mapping[str, Any]) -> str:
    return f"workers={row['workers']}" if "workers" in row else f"rate={row['rate']}"


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
