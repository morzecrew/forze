"""The soak's server half: scrape ``/metrics``, keep forze's series, and fit their drift.

A soak scrapes the app's Prometheus text once before sign-in and once per window. It keeps
the ``forze_*`` families and the process gauges ``instrument_process`` exports, turns each
counter into a per-second rate, and after the warm-up fits every series for upward drift.
A series is fitted only when it comes from one process: a scrape that lands on different
workers interleaves them, and a fit across processes is a confident false finding. Flags
never fail a run.
"""

from __future__ import annotations

import math
import re
import statistics
import time
from collections.abc import Mapping, Sequence
from typing import Any

import attrs
import httpx

from forze.base.exceptions import exc

# ----------------------- #

INSTANCE_LABEL = "service_instance_id"
"""The label the OTel Prometheus exporter renders ``service.instance.id`` as: on
``target_info`` always, and on every series of a pre-fork union (RFC 0083)."""

SNAPSHOT_AGE_FAMILY = "forze_metrics_snapshot_age_seconds"
"""The union route's own staleness gauge. It saws between scrapes and describes the scrape,
not the app, so it is never fitted."""

PROCESS_PREFIXES: tuple[str, ...] = ("process_cpu_time", "process_memory_usage")
"""``instrument_process``'s instruments as the exporter renders them
(``process_cpu_time_seconds_total``, ``process_memory_usage_bytes``)."""

CAPACITY: Mapping[str, str] = {
    "forze_cache_l1_size": "forze_cache_l1_capacity",
    "forze_tenancy_pool_size": "forze_tenancy_pool_capacity",
    "forze_connection_pool_in_use": "forze_connection_pool_max",
    "forze_connection_pool_idle": "forze_connection_pool_max",
    "forze_resilience_bulkhead_in_use": "forze_resilience_bulkhead_limit",
}
"""A gauge and the capacity it fills to. Once a gauge reaches its capacity at the same labels
it is not flagged: a cache or a pool filling up is not a leak."""

DRIFT_SHARE = 0.10
"""A series is flagged when its fitted change over the run passes this share of its first
quarter's mean."""

MIN_FIT_WINDOWS = 4
"""Windows after the warm-up a series needs before it is fitted: one per quarter."""

_AGE_RESOLUTION_S = 0.002
"""Slack on a snapshot's write time: the route renders its age to the millisecond, from a
write time it truncated to the millisecond."""

_NAME = re.compile(r"[a-zA-Z_:][a-zA-Z0-9_:]*")
_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:[^"\\]|\\.)*)"\s*')
_ESCAPE = re.compile(r"\\(.)")
_ESCAPES = frozenset('\\"n')
_TIMESTAMP = re.compile(r"[+-]?[0-9]+")
_SUFFIXES: tuple[str, ...] = ("_bucket", "_count", "_sum", "_total")
_NOT_FITTED = frozenset({"histogram", "summary"})


# ....................... #


@attrs.frozen
class MetricSample:
    """One sample line of a Prometheus text exposition."""

    name: str
    labels: Mapping[str, str]
    value: float
    type: str
    """The family's ``# TYPE``: ``counter``, ``gauge``, ``histogram``, ``summary`` or
    ``untyped`` (also for a sample no ``# TYPE`` line names)."""


def parse_metrics(text: str) -> list[MetricSample]:
    """Parse Prometheus text exposition format 0.0.4, as ``/metrics`` serves it.

    Label values are unescaped (``\\\\``, ``\\"``, ``\\n``, and no other escape); a
    timestamp must be a 64-bit integer and is then ignored; ``NaN`` and ``±Inf`` are read as
    floats. A histogram's ``_bucket``, ``_count`` and ``_sum`` samples take their family's
    type.

    :raises ValueError: a sample line is malformed. The whole exposition is refused, as
        Prometheus refuses the whole scrape. The message names the line by its number and
        says what is wrong with it, and never repeats text from it: a misrouted answer can
        carry anything, and the reason reaches the run file.
    """

    types: dict[str, str] = {}
    samples: list[tuple[str, dict[str, str], float]] = []

    for number, raw in enumerate(text.splitlines(), 1):
        line = raw.strip()

        if not line:
            continue

        if line.startswith("#"):
            parts = line.split(maxsplit=3)

            if len(parts) == 4 and parts[1] == "TYPE":
                types[parts[2]] = parts[3].strip()

            continue

        try:
            samples.append(_sample(line))

        except ValueError as error:
            raise ValueError(f"line {number} is not Prometheus text: {error}") from None

    return [
        MetricSample(name, labels, value, _type_of(name, types)) for name, labels, value in samples
    ]


def _sample(line: str) -> tuple[str, dict[str, str], float]:
    name = _NAME.match(line)

    if name is None:
        raise ValueError("it does not start with a metric name")

    labels: dict[str, str] = {}
    rest = line[name.end() :].lstrip()

    if rest.startswith("{"):
        rest = rest[1:].lstrip()

        while not rest.startswith("}"):
            label = _LABEL.match(rest)

            if label is None:
                raise ValueError("its labels are malformed")

            labels[label[1]] = _ESCAPE.sub(_unescape, label[2])
            rest = rest[label.end() :]

            if rest.startswith(","):
                rest = rest[1:].lstrip()

            elif not rest.startswith("}"):
                raise ValueError("its labels are malformed")

        rest = rest[1:]

    fields = rest.split()

    # A value, then an optional timestamp.
    if not 1 <= len(fields) <= 2:
        raise ValueError("it has no value, or more than a value and a timestamp")

    try:
        value = float(fields[0])

    except ValueError:
        raise ValueError("the value is not a number") from None

    if len(fields) == 2 and not _is_int64(fields[1]):
        raise ValueError("the timestamp is not a 64-bit integer")

    return name[0], labels, value


def _unescape(match: re.Match[str]) -> str:
    # Only three escapes exist; reading another as its character could merge two series.
    if match[1] not in _ESCAPES:
        raise ValueError('a label value has an escape other than \\\\, \\" or \\n')

    return "\n" if match[1] == "n" else match[1]


def _is_int64(token: str) -> bool:
    return _TIMESTAMP.fullmatch(token) is not None and -(2**63) <= int(token) < 2**63


def _type_of(name: str, types: Mapping[str, str]) -> str:
    if name in types:
        return types[name]

    for suffix in _SUFFIXES:
        if name.endswith(suffix) and name.removesuffix(suffix) in types:
            return types[name.removesuffix(suffix)]

    return "untyped"


def series_key(name: str, labels: Mapping[str, str]) -> str:
    """A series as one string, ``name{a="1",b="2"}`` with labels sorted.

    A label with an empty value is left out: Prometheus reads it as absent.
    """

    kept = sorted((key, value) for key, value in labels.items() if value)

    if not kept:
        return name

    return name + "{" + ",".join(f'{key}="{_escape(value)}"' for key, value in kept) + "}"


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _kept(sample: MetricSample) -> bool:
    """forze's own series and the process gauges; histograms, summaries and ``_created``
    timestamps are not fitted."""

    name = sample.name

    if sample.type in _NOT_FITTED or name.endswith("_created") or name == SNAPSHOT_AGE_FAMILY:
        return False

    return name.startswith("forze_") or name.startswith(PROCESS_PREFIXES)


# ....................... #


class ScrapeFailed(Exception):
    """A scrape got no readable Prometheus text; the message says why, without the body."""


async def fetch_metrics(client: httpx.AsyncClient, path: str) -> list[MetricSample]:
    """``GET`` *path* on the target, without a credential, and parse the answer.

    :raises ScrapeFailed: the request failed (in transport, a timeout, a body that cannot be
        decoded), answered other than 200, or is not Prometheus text.
    """

    try:
        response = await client.get(path, headers={"Accept": "text/plain"})

    except httpx.HTTPError as error:
        raise ScrapeFailed(f"request failed ({type(error).__name__})") from error

    if response.status_code != 200:
        raise ScrapeFailed(f"answered {response.status_code}")

    try:
        return parse_metrics(response.text)

    except ValueError as error:
        raise ScrapeFailed(str(error)) from error


@attrs.define
class ServerSeries:
    """Every scrape of one soak: the instances each named, and each kept series' points.

    A counter's point is its per-second rate since the previous scrape that carried it; a
    gauge's is its value. Points are kept per window, so a gauge lines up with its capacity.
    """

    client: httpx.AsyncClient
    path: str

    instances: list[frozenset[str]] = attrs.field(factory=lambda: list[frozenset[str]]())
    """The instance ids each successful scrape named, in order."""

    kinds: dict[str, str] = attrs.field(factory=lambda: dict[str, str]())
    labelled: set[str] = attrs.field(factory=lambda: set[str]())
    """Series that carry their own instance label, so come from one process by construction."""

    points: dict[str, dict[int, tuple[float, float]]] = attrs.field(
        factory=lambda: dict[str, dict[int, tuple[float, float]]]()
    )
    _last: dict[str, tuple[float, float]] = attrs.field(
        factory=lambda: dict[str, tuple[float, float]]()
    )
    _written: dict[str, tuple[float, float]] = attrs.field(
        factory=lambda: dict[str, tuple[float, float]]()
    )
    """Per instance, when the snapshot its last new point came from was written, as a span."""

    # ....................... #

    async def baseline(self) -> None:
        """The first scrape, before sign-in: a route that cannot be read refuses the run.

        :raises CoreException: ``configuration`` (``load_metrics_unavailable``).
        """

        try:
            sent = time.perf_counter()
            samples = await fetch_metrics(self.client, self.path)
            self.add(0, sent, samples, answered=time.perf_counter())

        except ScrapeFailed as error:
            raise exc.configuration(
                f"The soak's metrics route {self.path!r} cannot be read: {error}. Serve "
                "Prometheus text there "
                "(attach_metrics_route), or set LoadPlan.metrics=None for client numbers only",
                code="load_metrics_unavailable",
            ) from error

    async def sample(self, window: int) -> dict[str, Any]:
        """Scrape at the end of *window* and return the row's ``server`` record.

        A failed scrape is recorded as ``{"error": ...}`` and the run carries on: the next
        scrape's counter rates then span both windows.
        """

        try:
            sent = time.perf_counter()
            samples = await fetch_metrics(self.client, self.path)

        except ScrapeFailed as error:
            return {"error": str(error)}

        return self.add(window, sent, samples, answered=time.perf_counter())

    def add(
        self,
        window: int,
        t: float,
        samples: Sequence[MetricSample],
        *,
        answered: float | None = None,
    ) -> dict[str, Any]:
        """Record one scrape sent at *t* and answered at *answered* seconds (*t* by default),
        and return what the row shows of it.

        A point is stamped at *t*, before the route renders. A pre-fork union serves each
        worker's last snapshot, up to its interval old, so a series that names its instance
        is stamped at that snapshot's write time instead: *t* less the instance's
        ``forze_metrics_snapshot_age_seconds``. When the snapshot is the one the previous
        new point came from, the instance's series get no point: a second read of one
        snapshot would give a counter a rate of 0, and the next read double.
        """

        answered = t if answered is None else answered
        instances = frozenset(
            sample.labels[INSTANCE_LABEL] for sample in samples if sample.labels.get(INSTANCE_LABEL)
        )
        self.instances.append(instances)
        ages = {
            sample.labels[INSTANCE_LABEL]: sample.value
            for sample in samples
            if sample.name == SNAPSHOT_AGE_FAMILY
            and sample.labels.get(INSTANCE_LABEL)
            and math.isfinite(sample.value)
        }
        seen = {
            instance for instance, age in ages.items() if self._seen(instance, t, answered, age)
        }
        series: dict[str, float | None] = {}

        for sample in filter(_kept, samples):
            key = series_key(sample.name, sample.labels)
            kind = "counter" if sample.type == "counter" else "gauge"
            instance = sample.labels.get(INSTANCE_LABEL, "")
            self.kinds[key] = kind
            value: float | None = sample.value
            stamp = t - ages.get(instance, 0.0)

            if instance:
                self.labelled.add(key)

            if instance in seen:
                series[key] = None
                continue

            if kind == "counter":
                # A cumulative series rises under any steady load: fit its rate, not its level.
                last = self._last.get(key)
                self._last[key] = (stamp, sample.value)
                value = (
                    None
                    if last is None or stamp <= last[0] or sample.value < last[1]  # a reset
                    else (sample.value - last[1]) / (stamp - last[0])
                )

            if value is None or not math.isfinite(value):
                series[key] = None
                continue

            self.points.setdefault(key, {})[window] = (stamp, value)
            series[key] = _round(value)

        return {"instances": sorted(instances), "series": series}

    def _seen(self, instance: str, sent: float, answered: float, age: float) -> bool:
        """Whether *instance*'s snapshot is the one its last new point came from.

        The route reads the age somewhere between sending and answering, so the write lies in
        ``[sent - age, answered - age]``. Two reads whose spans overlap saw one snapshot, as
        long as the worker writes less often than two scrapes take.
        """

        span = (sent - age, answered - age)
        previous = self._written.get(instance)

        if previous is not None and span[0] <= previous[1] + _AGE_RESOLUTION_S:
            return True

        self._written[instance] = span

        return False

    def attribution(self) -> str:
        """How the first scrape ties its series to a process.

        ``per_series`` when each series names its instance (a pre-fork union), ``instance``
        when the scrape names one, ``interleaved`` when it names several and its series do
        not say which, ``none`` when it names none. With the last two no server series can
        be fitted, which the run's meta line records before the load starts.
        """

        first = self.instances[0] if self.instances else frozenset[str]()

        if self.kinds and self.kinds.keys() <= self.labelled:
            return "per_series"

        if len(first) == 1:
            return "instance"

        return "interleaved" if first else "none"

    # ....................... #

    def drift(self, warm_windows: int, duration_s: float) -> dict[str, dict[str, Any]]:
        """Fit every series past the warm-up, or say why it is not fitted.

        A series without its own instance label belongs to whichever process answered each
        scrape. It is fitted only when every scrape named one and the same instance:
        ``interleaved`` when they named several or different ones, ``unattributed`` when
        none named any.
        """

        named = {instance for scrape in self.instances for instance in scrape}
        one_process = len(named) == 1 and all(len(scrape) == 1 for scrape in self.instances)
        report: dict[str, dict[str, Any]] = {}

        for key, kind in sorted(self.kinds.items()):
            if key not in self.labelled and not one_process:
                report[key] = {"type": kind, "status": "interleaved" if named else "unattributed"}
                continue

            points = sorted(
                (window, point)
                for window, point in self.points.get(key, {}).items()
                if window > warm_windows
            )
            result = fit([point for _, point in points], duration_s)

            if result["status"] == "flagged" and self._at_capacity(key, points):
                result["status"] = "at_capacity"

            report[key] = {"type": kind, **result}

        return report

    def _at_capacity(self, key: str, points: Sequence[tuple[int, tuple[float, float]]]) -> bool:
        name, brace, labels = key.partition("{")
        capacity = CAPACITY.get(name)

        if capacity is None:
            return False

        caps = self.points.get(capacity + brace + labels, {})

        return any(window in caps and value >= caps[window][1] for window, (_, value) in points)


# ....................... #


def fit(points: Sequence[tuple[float, float]], duration_s: float) -> dict[str, Any]:
    """Fit one series' ``(time, value)`` points: an OLS slope and the first and last quarters.

    Flagged when the fitted change over the run, the slope times *duration_s*, passes
    :data:`DRIFT_SHARE` of the first quarter's mean and the last quarter is above the first.
    From a first quarter of zero it is flagged only when every point of the last quarter is
    above zero, and is ``sparse`` otherwise.
    The change is in the series' own unit, so neither the window length nor the time unit
    moves the threshold, as long as *duration_s* is in the unit of the times.
    """

    if len(points) < MIN_FIT_WINDOWS:
        return {"points": len(points), "status": "too_few"}

    times = [t for t, _ in points]
    values = [value for _, value in points]

    try:
        slope = statistics.linear_regression(times, values).slope

    except statistics.StatisticsError:  # every point at one time: there is no slope to read
        return {"points": len(points), "status": "inconclusive"}

    quarter = len(values) // 4
    first = statistics.fmean(values[:quarter])
    last = statistics.fmean(values[-quarter:])
    change = slope * duration_s
    status = "flagged" if change > DRIFT_SHARE * abs(first) and last > first else "steady"

    # From a first quarter of zero any rise passes 10 %, so one late event would read as a
    # leak: it counts only when every point of the last quarter is above zero.
    if status == "flagged" and first == 0 and not all(v > 0 for v in values[-quarter:]):
        status = "sparse"

    return {
        "points": len(points),
        "slope_per_s": _round(slope),
        "change": _round(change),
        "first_quarter": _round(first),
        "last_quarter": _round(last),
        "status": status,
    }


def drift_report(
    rows: Sequence[Mapping[str, Any]],
    ends: Sequence[float],
    server: ServerSeries | None,
    *,
    warm_windows: int,
) -> dict[str, Any]:
    """The soak summary's ``drift``: client latency and every server series, fitted.

    *ends* holds each row's window end, in seconds since the load started, at full
    precision: the rows' ``t_s`` is rounded for the file. The run's duration is the last
    window's end. Client latency percentiles are fitted from the window rows; a window where
    no call answered has no point, and neither has a window a stop cut short, whose few
    calls one slow answer can dominate.
    """

    duration_s = ends[-1] if ends else 0.0
    fitted = [
        (end, row)
        for row, end in zip(rows, ends, strict=True)
        if row["window"] > warm_windows and not row.get("cut_short")
    ]
    client = {
        name: fit(
            [(end, row["ms"][name]) for end, row in fitted if row["ms"][name] is not None],
            duration_s,
        )
        for name in ("p50", "p95", "p99")
    }
    servers = server.drift(warm_windows, duration_s) if server is not None else None
    flagged = [f"client.{name}" for name, result in client.items() if result["status"] == "flagged"]
    flagged.extend(key for key, result in (servers or {}).items() if result["status"] == "flagged")

    return {
        "duration_s": round(duration_s, 3),
        "warm_windows": warm_windows,
        "flagged": flagged,
        "client": client,
        "server": servers,
    }


def _round(value: float) -> float:
    return float(f"{value:.6g}")
