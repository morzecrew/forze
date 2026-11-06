"""The run engine: closed and open load models, driven step by step against a live app."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import itertools
import math
import os
import platform
import random
import time
from collections.abc import Awaitable, Callable, Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import attrs
import httpx

from forze.base.exceptions import exc
from forze.base.primitives import utcnow

from ._seams import process_seconds, seeded_random
from ._target import no_cookies, target_guard
from ._version import __version__
from .auth import Auth, AuthSession, HeadersFn
from .catalog import Catalog, SampleExhausted, resolve
from .plan import (
    TENANT_HEADER,
    Call,
    LoadPlan,
    Principal,
    Sample,
    Scenario,
    safe_target,
    validate_plan,
)
from .report import SCHEMA, RunWriter
from .soak import MIN_FIT_WINDOWS, Fetched, ServerSeries, drift_report
from .stats import StepRecorder, classify, error_share, find_knee

# ----------------------- #

LOGIN_CONCURRENCY = 4
"""Principals signed in at once: forze's default ``hashing_concurrency``, so the run's start
does not queue behind Argon2 and measure the queue."""

SATURATED_CPU = 0.85
"""Event-loop thread CPU, in cores, at which a step's client counts as saturated."""

SATURATED_LAG_MS = 25.0
"""Event-loop lag p99 at which a step's client counts as saturated."""

SATURATED_BEHIND = 0.01
"""Share of open-model sends started late at which a step's client counts as saturated."""

CAP_TAIL = 1e-6
"""The open model's in-flight cap is the ``1 - CAP_TAIL`` quantile of a Poisson count."""

_PROBE_S = 0.05
_BEHIND_S = 0.01
_DRAIN_MARGIN_S = 1.0


# ....................... #


def _ints(values: Iterable[int]) -> tuple[int, ...]:
    return tuple(values)


def _floats(values: Iterable[float]) -> tuple[float, ...]:
    # Floats whatever was given, so a row's `rate` reads the same from Python and the CLI.
    return tuple(float(value) for value in values)


def _positive_steps(values: Sequence[float], what: str) -> None:
    # `nan <= 0` is false, so finiteness is checked first: a nan or inf rate would crash the
    # in-flight cap's quantile instead of being refused.
    if not values or not all(math.isfinite(value) and value > 0 for value in values):
        raise exc.configuration(f"{what} must be a non-empty list of positive, finite numbers")


@attrs.frozen(kw_only=True)
class _Ramp:
    step_s: float
    """Seconds per step, warm-up included."""

    warm_s: float = 0.0
    """Seconds at the start of each step left out of its numbers."""

    stop_error_rate: float = 0.05
    """Share of calls answering 5xx (504 included) or failing in transport that stops the
    ramp."""

    def __attrs_post_init__(self) -> None:
        if not math.isfinite(self.step_s) or not 0.0 <= self.warm_s < self.step_s:
            raise exc.configuration("step_s must be finite, and warm_s at least 0 and below it")

        # A nan threshold would make every comparison false and never stop the ramp.
        if not 0.0 <= self.stop_error_rate <= 1.0:
            raise exc.configuration("stop_error_rate must be a share between 0 and 1")


@attrs.frozen(kw_only=True)
class ClosedRamp(_Ramp):
    """Closed model: *workers* send, wait for the answer, and send again, step by step.

    It finds capacity. Its latency is service time under a closed loop, and a server stall
    is recorded once per worker instead of once per request that should have been sent, so
    rows are labelled ``"latency": "closed"``.
    """

    steps: tuple[int, ...] = attrs.field(converter=_ints)
    """Workers per step."""

    think_s: float = 0.0
    """Pause after each answer, per worker."""

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        _positive_steps(self.steps, "steps")

        # Worker counts: a fractional step would run a rounded-up count and report the fraction.
        if not all(isinstance(n, int) and not isinstance(n, bool) for n in self.steps):
            raise exc.configuration("steps must be whole worker counts")

        if not (math.isfinite(self.think_s) and self.think_s >= 0):
            raise exc.configuration("think_s must be a finite number of seconds, at least 0")


@attrs.frozen(kw_only=True)
class OpenRamp(_Ramp):
    """Open model: arrivals at a rate per step, whatever the answers do.

    Latency runs from each request's intended send time, so a stall shows in every request
    scheduled during it. In-flight requests are capped at :func:`inflight_cap`; a send
    refused at the cap counts as a call and as ``transport``.
    """

    rates: tuple[float, ...] = attrs.field(converter=_floats)
    """Arrivals per second, per step."""

    arrivals: Literal["constant", "poisson"] = "constant"
    """Evenly spaced arrivals, or a Poisson process."""

    def __attrs_post_init__(self) -> None:
        super().__attrs_post_init__()
        _positive_steps(self.rates, "rates")


@attrs.frozen(kw_only=True)
class Soak:
    """A steady load for *duration_s*, one row per window, and drift flagged at the end.

    The load is closed (*workers*) or open (*rate*), never both. Each window's row records
    goodput beside latency, the client self-report, and the server's series scraped from
    :attr:`~forze_load.LoadPlan.metrics`. After *warm_windows* windows every series is
    fitted for upward drift (:func:`forze_load.soak.fit`). Flags never fail the run.
    """

    duration_s: float
    """Seconds of load, rounded down to whole windows, warm-up included."""

    window_s: float = 60.0
    """Seconds per window: one row, and one scrape, each."""

    warm_windows: int = 2
    """Windows at the start written as rows, marked ``warm``, and left out of the fit."""

    workers: int | None = None
    """Closed model: workers that send, wait and send again."""

    rate: float | None = attrs.field(default=None, converter=attrs.converters.optional(float))
    """Open model: arrivals per second."""

    think_s: float = 0.0
    """Closed model: pause after each answer, per worker."""

    arrivals: Literal["constant", "poisson"] = "constant"
    """Open model: evenly spaced arrivals, or a Poisson process."""

    stop_error_rate: float = 0.05
    """Share of a window's calls answering 5xx (504 included) or failing in transport that
    stops the soak."""

    def __attrs_post_init__(self) -> None:
        if (self.workers is None) == (self.rate is None):
            raise exc.configuration("a soak takes workers (closed) or rate (open), exactly one")

        if self.workers is not None and (
            isinstance(self.workers, bool) or not isinstance(self.workers, int) or self.workers < 1
        ):
            raise exc.configuration("workers must be a whole number of at least 1")

        if self.rate is not None and not (math.isfinite(self.rate) and self.rate > 0):
            raise exc.configuration("rate must be a positive, finite number of arrivals a second")

        if not (math.isfinite(self.window_s) and self.window_s > 0):
            raise exc.configuration("window_s must be a positive, finite number of seconds")

        if isinstance(self.warm_windows, bool) or not isinstance(self.warm_windows, int):
            raise exc.configuration("warm_windows must be a whole number")

        # RFC 0084 §5.8: drift is fitted only after a warm-up of at least two windows.
        if self.warm_windows < 2:
            raise exc.configuration("warm_windows must be at least 2")

        needed = self.warm_windows + MIN_FIT_WINDOWS

        if not math.isfinite(self.duration_s) or self.windows < needed:
            raise exc.configuration(
                f"a soak needs at least {needed} windows, {self.warm_windows} of warm-up and "
                f"{MIN_FIT_WINDOWS} to fit: raise duration_s or lower window_s"
            )

        if not (math.isfinite(self.think_s) and self.think_s >= 0):
            raise exc.configuration("think_s must be a finite number of seconds, at least 0")

        if not 0.0 <= self.stop_error_rate <= 1.0:
            raise exc.configuration("stop_error_rate must be a share between 0 and 1")

    @property
    def windows(self) -> int:
        """Whole windows the soak runs."""

        # Floor with a little slack: 0.3 / 0.05 is 5.999… in binary floating point.
        return math.floor(self.duration_s / self.window_s + 1e-9)


type Profile = ClosedRamp | OpenRamp | Soak


@attrs.frozen
class RunResult:
    """A finished run: the meta record, one row per step, and the summary."""

    meta: dict[str, Any]
    rows: list[dict[str, Any]]
    summary: dict[str, Any]


# ....................... #


@functools.lru_cache(maxsize=256)
def inflight_cap(rate: float, timeout_s: float) -> int:
    """The open model's in-flight cap: the ``1 - CAP_TAIL`` quantile of a Poisson count.

    Its mean is rate × timeout, the number in flight when every request takes the whole
    timeout. The mean itself would refuse sends to a healthy server, because Poisson arrivals
    bunch; the quantile refuses only when a real backlog has built up. It is 9 at a mean of 1,
    151 at 100 and 1,154 at 1,000.
    """

    mean = rate * timeout_s

    if mean <= 0:
        return 1

    target = 1.0 - CAP_TAIL
    log_mean = math.log(mean)
    # Below twelve standard deviations under the mean the mass is negligible, so start there.
    k = max(0, math.floor(mean - 12.0 * math.sqrt(mean)))
    total = 0.0

    while True:
        total += math.exp(k * log_mean - mean - math.lgamma(k + 1))

        if total >= target:
            return max(1, k)

        k += 1


async def run(
    plan: LoadPlan,
    profile: Profile,
    *,
    out: str | Path | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
    on_meta: Callable[[Mapping[str, Any]], None] | None = None,
) -> RunResult:
    """Drive *plan* against its target with *profile*, and write JSON lines to *out*.

    The plan is validated first, so a refused write or a bad mix stops the run before
    anything is sent. Operation references are then resolved from the app's OpenAPI
    document, which is the only request sent before sign-in. Principals are signed in, at
    most :data:`LOGIN_CONCURRENCY` at once, and the first failure stops the run before any
    load; ids are then sampled as each principal that uses them. Rows are written as each
    step ends, so an interrupted run keeps the steps it finished. A ramp that stops on its
    stop rule, or because a principal's samples all ran out, is a finished run: the summary
    says where and why.

    A :class:`Soak` writes a row per window instead of per step, with no gap between
    windows. It scrapes the plan's ``metrics`` route once before sign-in and as each window
    closes, beside the next one, and ends with a summary of drift instead of a knee. Its meta line says how the first scrape ties series to a
    process (``server.attribution``); ``none`` or ``interleaved`` means no server series
    will be fitted.

    :param plan: Target, principals and scenarios.
    :param profile: :class:`ClosedRamp`, :class:`OpenRamp` or :class:`Soak`.
    :param out: JSON lines file to write, or ``None`` to only return the result.
    :param transport: An ``httpx`` transport to use instead of the network. An app served
        through it on the same event loop runs on the loop's thread, so the loop's CPU is
        then reported but not judged (``loop_cpu_judged: false``).
    :param on_meta: Called with the meta record once it is written, before sign-in, so a
        caller can warn at the start of a long run.
    :returns: The meta record, the rows and the summary.
    :raises CoreException: ``configuration`` — the plan is refused (:func:`validate_plan`),
        an operation reference does not resolve (:func:`forze_load.catalog.resolve`), a
        soak's metrics route cannot be read, a credential is missing, a sign-in fails, a
        sample fails or comes back empty, a refresh fails after the token expired, or a
        scenario passes :meth:`Call.request` a URL with a host.
    """

    validate_plan(plan)
    model, levels = _levels(profile)
    connections = (
        int(max(levels))
        if model == "closed"
        else max(inflight_cap(rate, plan.timeout_s) for rate in levels)
    )

    async with (
        httpx.AsyncClient(
            base_url=plan.base_url,
            timeout=plan.timeout_s,
            # httpx's default pool is 100 connections: above that, requests would queue in
            # the client and be measured as server latency.
            limits=httpx.Limits(max_connections=connections, max_keepalive_connections=connections),
            transport=transport,
            # Both clients are shared by every principal: they keep no cookie, and refuse any
            # request off the plan's target, so no credential reaches another principal or
            # another host.
            cookies=no_cookies(),
            event_hooks={"request": [target_guard(plan.base_url)]},
        ) as client,
        # Sign-in and refresh get their own pool: a refresh must never wait behind a full
        # request pool, and its time is the credential's, not the request's.
        httpx.AsyncClient(
            base_url=plan.base_url,
            timeout=plan.timeout_s,
            transport=transport,
            cookies=no_cookies(),
            event_hooks={"request": [target_guard(plan.base_url)]},
        ) as auth_client,
    ):
        catalog = await resolve(plan, auth_client)
        server = (
            ServerSeries(auth_client, plan.metrics)
            if isinstance(profile, Soak) and plan.metrics is not None
            else None
        )

        # Before sign-in, like the document: a route that is not there refuses the run.
        if server is not None:
            await server.baseline()

        state = _Run(
            plan=plan,
            catalog=catalog,
            client=client,
            auth_client=auth_client,
            cpu_judged=transport is None,
        )
        meta = _meta(plan, profile, model, catalog)

        # Known from the first scrape: a soak whose series no scrape can tie to one process
        # says so now, not when its drift comes back empty hours later.
        if server is not None:
            meta["server"] = {"attribution": server.attribution()}

        with RunWriter(out) as writer:
            writer.write(meta)

            if on_meta is not None:
                on_meta(meta)

            await state.sign_in()
            await state.sample()
            probe = asyncio.create_task(state.probe.run())

            try:
                if isinstance(profile, ClosedRamp):
                    rows, stopped = await _closed(state, profile, writer)

                elif isinstance(profile, OpenRamp):
                    rows, stopped = await _open(state, profile, writer)

                else:
                    rows, stopped, ends = await _soak(state, profile, writer, server)

            finally:
                probe.cancel()
                await asyncio.gather(probe, return_exceptions=True)

            if isinstance(profile, Soak):
                summary = {
                    "kind": "summary",
                    "windows": len(rows),
                    "stopped": stopped,
                    "drift": drift_report(rows, ends, server, warm_windows=profile.warm_windows),
                }

            else:
                # A step a stop cut short measured a shorter window and a smaller mix: no knee.
                knee = find_knee([row for row in rows if not row.get("cut_short")], model=model)
                summary = {
                    "kind": "summary",
                    "steps": len(rows),
                    "knee": knee["knee"],
                    "knee_confirmed": knee["confirmed"],
                    "note": knee["note"],
                    "stopped": stopped,
                }

            writer.write(summary)

    return RunResult(meta=meta, rows=rows, summary=summary)


# ....................... #


@attrs.define
class _Probe:
    """The client's own load: event-loop thread CPU, process CPU and event-loop lag."""

    lags: list[float] = attrs.field(factory=lambda: list[float]())
    _wall: float = 0.0
    _thread: float = 0.0
    _process: float = 0.0

    async def run(self) -> None:
        while True:
            started = time.perf_counter()
            await asyncio.sleep(_PROBE_S)
            self.lags.append(max(time.perf_counter() - started - _PROBE_S, 0.0))

    def begin(self) -> None:
        self.lags.clear()
        self._wall = time.perf_counter()
        # The loop thread's CPU, not the process's: one event loop runs Python on one core.
        # An app served by another thread of this process does not count; an app served on
        # this loop through `transport=` does, which is why the run then does not judge it.
        self._thread = time.thread_time()
        self._process = process_seconds()

    def report(self, scheduled: int, behind: int, *, cpu_judged: bool) -> dict[str, Any]:
        wall = max(time.perf_counter() - self._wall, 1e-9)
        loop_cpu = (time.thread_time() - self._thread) / wall
        lags = sorted(self.lags)
        lag_p99 = lags[min(len(lags) - 1, int(0.99 * len(lags)))] * 1000 if lags else 0.0
        behind_share = behind / scheduled if scheduled else 0.0

        return {
            "loop_cpu": round(loop_cpu, 3),
            "loop_cpu_judged": cpu_judged,
            "process_cpu": round((process_seconds() - self._process) / wall, 3),
            "loop_lag_p99_ms": round(lag_p99, 2),
            "behind_schedule": behind,
            "saturated": (cpu_judged and loop_cpu >= SATURATED_CPU)
            or lag_p99 > SATURATED_LAG_MS
            or behind_share > SATURATED_BEHIND,
        }


# ....................... #


@attrs.define
class _Session:
    """One principal's credential for this run, shared by every worker that runs as it."""

    principal: Principal
    client: httpx.AsyncClient
    credential: AuthSession | HeadersFn | None = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        auth = self.principal.auth
        self.credential = auth.session() if isinstance(auth, Auth) else auth

    async def headers(self) -> dict[str, str]:
        credential = self.credential
        headers: dict[str, str] = {}

        if isinstance(credential, AuthSession):
            headers.update(await credential.headers(self.client))

        elif credential is not None:
            headers.update(await credential(self.client))

        if self.principal.tenant is not None:
            headers[TENANT_HEADER] = self.principal.tenant

        return headers


@attrs.define
class _Run:
    plan: LoadPlan
    catalog: Catalog
    client: httpx.AsyncClient
    auth_client: httpx.AsyncClient
    cpu_judged: bool
    probe: _Probe = attrs.field(factory=_Probe)
    current: StepRecorder | None = None
    failure: BaseException | None = None
    halt: dict[str, Any] | None = None
    """Why the run stops early without failing: a principal with nothing left to call."""

    interrupted: asyncio.Event = attrs.field(factory=asyncio.Event)
    exhausted: list[dict[str, str]] = attrs.field(factory=lambda: list[dict[str, str]]())
    """Scenarios retired since the last row because 404s emptied their sample."""

    sessions: dict[str, _Session] = attrs.field(init=False)
    scenarios: dict[str, Scenario] = attrs.field(init=False)
    weights: dict[str, dict[str, float]] = attrs.field(init=False)
    mixes: dict[str, tuple[list[str], list[float]]] = attrs.field(init=False)
    rng: random.Random = attrs.field(init=False)
    arrival_rng: random.Random = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        # Picks and `Call.rng` share a stream; Poisson arrivals draw from their own, so a
        # scenario that draws more or fewer numbers does not move the arrival times.
        self.rng = seeded_random(self.plan.seed, "picks")
        self.arrival_rng = seeded_random(self.plan.seed, "arrivals")
        self.sessions = {p.name: _Session(p, self.auth_client) for p in self.plan.principals}
        self.scenarios = {s.name: s for s in self.catalog.scenarios}
        self.weights = {p.name: self.catalog.expand(p.mix) for p in self.plan.principals}
        self.mixes = {name: _cumulative(weights) for name, weights in self.weights.items()}
        self.catalog.pool.on_exhausted = self.retire

    # ....................... #

    async def sign_in(self) -> None:
        sessions = list(self.sessions.values())

        for start in range(0, len(sessions), LOGIN_CONCURRENCY):
            batch = sessions[start : start + LOGIN_CONCURRENCY]
            await asyncio.gather(*(session.headers() for session in batch))

    async def sample(self) -> None:
        """Read every sample as each principal whose mix uses it, before the first step."""

        for principal in self.plan.principals:
            needed = {
                sample
                for name in self.weights[principal.name]
                for sample in self.catalog.samples_of.get(name, ())
            }

            for sample in sorted(needed, key=_sample_order):
                # Per sample, so a long sampling phase refreshes the token like any call.
                headers = await self.sessions[principal.name].headers()
                await self.catalog.collect(self.client, sample, principal.name, headers)

    def pick(self, principal: Principal) -> str | None:
        """A scenario from *principal*'s live mix, or ``None`` once nothing is left in it."""

        names, cumulative = self.mixes[principal.name]

        return self.rng.choices(names, cum_weights=cumulative)[0] if names else None

    def retire(self, principal: str, sample: Sample) -> None:
        """404s emptied *principal*'s values for *sample*: drop the scenarios that use it."""

        weights = self.weights[principal]

        for name in [n for n in weights if sample in self.catalog.samples_of.get(n, ())]:
            del weights[name]
            self.exhausted.append({"principal": principal, "scenario": name})

        self.mixes[principal] = _cumulative(weights)

        if not weights:
            self.stop({"reason": "sample_exhausted", "principal": principal})

    def measuring(self) -> StepRecorder | None:
        """The step a call sent now counts in, or ``None`` outside a measured window."""

        step = self.current

        return step if step is not None and time.perf_counter() >= step.warm_until else None

    def fail(self, error: BaseException) -> None:
        if self.failure is None:
            self.failure = error
            self.interrupted.set()

    def stop(self, reason: dict[str, Any]) -> None:
        """End the current step now and the run after it; the summary records *reason*."""

        if self.halt is None:
            self.halt = reason
            self.interrupted.set()

    # ....................... #

    async def call(self, principal: Principal, started: float, step: StepRecorder | None) -> None:
        """Send one request as *principal*; latency runs from *started*, auth time excluded.

        *step* is the step current when the call was sent (``None`` outside a measured
        window), so a slow answer counts in the step that sent it. A call that got no answer
        (a transport failure) is counted but has no latency: percentiles cover answered
        calls.
        """

        name = self.pick(principal)

        if name is None:  # its samples ran out: the run is stopping
            return

        # Joined before the credential is awaited: a call still waiting on a refresh when its
        # step closes must hold the step's row open, not record into a row already written.
        if step is not None:
            step.enter()

        try:
            auth_started = time.perf_counter()
            headers = await self.sessions[principal.name].headers()
            auth_s = time.perf_counter() - auth_started

            if step is not None:
                step.add_auth(auth_s)

            call = Call(client=self.client, headers=headers, principal=principal.name, rng=self.rng)

            try:
                response = await self.scenarios[name].send(call)
                outcome = classify(response.status_code)
                latency: float | None = (time.perf_counter() - started - auth_s) * 1000

            except httpx.TransportError:  # timeouts included
                outcome, latency = "transport", None

            except SampleExhausted:  # its sample emptied while it waited: nothing was sent
                return

            if step is not None:
                step.record(name, outcome, latency)

        finally:
            if step is not None:
                step.leave()

    async def guarded(
        self, principal: Principal, started: float, step: StepRecorder | None
    ) -> None:
        try:
            await self.call(principal, started, step)

        except Exception as error:  # a scenario's own bug stops the run, loudly
            self.fail(error)

    async def worker(self, principal: Principal, think_s: float) -> None:
        while self.failure is None and self.halt is None:
            await self.guarded(principal, time.perf_counter(), self.measuring())
            # Always yield: a scenario that answers without suspending (a cached response, a
            # fake transport) would otherwise hold the loop and starve the step's timer.
            await asyncio.sleep(think_s)

    async def arrivals(
        self,
        rate: float,
        kind: str,
        inflight: set[asyncio.Task[None]],
        assign: Iterator[Principal],
    ) -> None:
        cap = inflight_cap(rate, self.plan.timeout_s)
        start = due = time.perf_counter()
        sent = 0

        while True:
            if kind == "poisson":
                due += self.arrival_rng.expovariate(rate)

            else:
                due = start + sent / rate

            sent += 1
            # Yields even when behind, so the sends already started get to run.
            await asyncio.sleep(max(due - time.perf_counter(), 0.0))

            # A halted run sends nothing more: the step drains, and no row would count it.
            if self.halt is not None:
                return

            step = self.measuring()

            if step is not None:
                step.scheduled += 1
                step.behind += int(time.perf_counter() - due > _BEHIND_S)

            principal = next(assign)

            if len(inflight) >= cap:
                name = self.pick(principal)

                if step is not None and name is not None:
                    step.record(name, "transport", None)

                continue

            task = asyncio.create_task(self.guarded(principal, due, step))
            inflight.add(task)
            task.add_done_callback(inflight.discard)

    # ....................... #

    async def step(
        self,
        profile: _Ramp,
        key: Mapping[str, Any],
        latency: str,
    ) -> dict[str, Any]:
        """Run one step's window and return its row; warm-up is left out of the numbers.

        Calls sent after the window closes count nowhere until the next step opens. The row
        waits (up to the request timeout) for the calls the step sent, and its rates use
        the measured window's real length. A :meth:`stop` ends the window early, and the
        row then says ``cut_short``.
        """

        opened = time.perf_counter()
        step = StepRecorder(warm_until=opened + profile.warm_s)
        self.current = step
        await self._sleep_until(step.warm_until)
        self.probe.begin()
        await self._sleep_until(opened + profile.step_s)
        cut_short = self.halt is not None  # stopped before the window's end
        client = self.probe.report(step.scheduled, step.behind, cpu_judged=self.cpu_judged)
        measured = max(time.perf_counter() - step.warm_until, 0.0)
        self.current = None
        await step.drain(self.plan.timeout_s + _DRAIN_MARGIN_S)

        if self.failure is not None:
            raise self.failure

        summary = step.summary(measured)
        scenarios = summary.pop("scenarios")
        row = {
            "kind": "step",
            **key,
            "latency": latency,
            **summary,
            "client": client,
            "scenarios": scenarios,
        }

        if self.exhausted:
            row["sample_exhausted"], self.exhausted = self.exhausted, []

        if cut_short:
            row["cut_short"] = True

        return row

    async def _sleep_until(self, deadline: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                self.interrupted.wait(), timeout=max(deadline - time.perf_counter(), 0.0)
            )

        if self.failure is not None:
            raise self.failure


# ....................... #


async def _ramp(
    state: _Run,
    profile: _Ramp,
    keys: Sequence[Mapping[str, Any]],
    prepare: Callable[[Mapping[str, Any]], Awaitable[None]],
    latency: str,
    writer: RunWriter,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    rows: list[dict[str, Any]] = []

    for key in keys:
        await prepare(key)
        row = await state.step(profile, key, latency)
        rows.append(row)
        writer.write(row)

        if state.halt is not None:
            return rows, {**_step_key(row), **state.halt}

        stopped = _stop_reason(row, profile.stop_error_rate)

        if stopped is not None:
            return rows, stopped

    return rows, None


async def _closed(
    state: _Run,
    profile: ClosedRamp,
    writer: RunWriter,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    workers: list[asyncio.Task[None]] = []
    assign = _assignments(state.plan.principals)

    async def prepare(key: Mapping[str, Any]) -> None:
        while len(workers) < key["workers"]:
            workers.append(asyncio.create_task(state.worker(next(assign), profile.think_s)))

        while len(workers) > key["workers"]:
            workers.pop().cancel()

    try:
        keys = [{"workers": n} for n in profile.steps]

        return await _ramp(state, profile, keys, prepare, "closed", writer)

    finally:
        for task in workers:
            task.cancel()

        await asyncio.gather(*workers, return_exceptions=True)


async def _open(
    state: _Run,
    profile: OpenRamp,
    writer: RunWriter,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    inflight: set[asyncio.Task[None]] = set()
    assign = _assignments(state.plan.principals)
    drivers: list[asyncio.Task[None]] = []

    async def prepare(key: Mapping[str, Any]) -> None:
        for driver in drivers:
            driver.cancel()

        await asyncio.gather(*drivers, return_exceptions=True)
        drivers[:] = [
            asyncio.create_task(state.arrivals(key["rate"], profile.arrivals, inflight, assign))
        ]

    try:
        keys = [{"rate": rate} for rate in profile.rates]

        return await _ramp(state, profile, keys, prepare, "open", writer)

    finally:
        for task in (*drivers, *inflight):
            task.cancel()

        await asyncio.gather(*drivers, *inflight, return_exceptions=True)


_DONE: dict[str, Any] = {"reason": "done"}
"""The halt a soak sets at its last boundary: no new sends, while calls in flight drain."""


async def _soak(
    state: _Run,
    profile: Soak,
    writer: RunWriter,
    server: ServerSeries | None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None, list[float]]:
    """One load level, window after window, with no gap between them.

    The load starts once. Windows close on a fixed schedule from its start, whatever the
    drains and scrapes take, and the next opens at the same instant, so every call counts in
    a window and open-model arrivals keep their phase. A closed window's calls drain and its
    scrape runs from its close, beside the windows after it, each within ``timeout_s``.
    Rows are written in window order, each once its own drain and scrape are done, and the
    stop rule is read on each as it is written. A stop ends the window then running, which
    is written cut short. At the last boundary, or once a stop is known, no call is sent any
    more; the calls in flight drain into the window that sent them. Returns the rows, why
    the soak stopped, and each window's end in seconds since the load started, at full
    precision, for the fit.
    """

    model, (level,) = _levels(profile)
    key: dict[str, Any] = {"workers": int(level)} if model == "closed" else {"rate": level}
    assign = _assignments(state.plan.principals)
    inflight: set[asyncio.Task[None]] = set()
    load = (
        [
            asyncio.create_task(state.worker(next(assign), profile.think_s))
            for _ in range(int(level))
        ]
        if model == "closed"
        else [asyncio.create_task(state.arrivals(level, profile.arrivals, inflight, assign))]
    )
    rows: list[dict[str, Any]] = []
    ends: list[float] = []
    closing: list[asyncio.Task[tuple[dict[str, Any], Fetched | None]]] = []
    closed: asyncio.Queue[int | None] = asyncio.Queue()
    rule: list[dict[str, Any]] = []
    started = time.perf_counter()

    def check() -> None:
        if state.failure is not None:
            raise state.failure

    def close(window: int, recorder: StepRecorder, *, cut_short: bool) -> None:
        ends.append(time.perf_counter() - started)
        client = state.probe.report(
            recorder.scheduled, recorder.behind, cpu_judged=state.cpu_judged
        )
        measured = max(time.perf_counter() - recorder.warm_until, 0.0)
        row = {
            "kind": "window",
            "window": window,
            **key,
            "latency": model,
            "warm": window <= profile.warm_windows,
            "t_s": round(ends[-1], 3),
        }
        closing.append(
            asyncio.create_task(
                _window_row(state, server, recorder, row, client, measured, cut_short=cut_short)
            )
        )
        closed.put_nowait(len(closing) - 1)

    async def write_rows() -> None:
        """Write the closed windows' rows in order, and read the stop rule on each."""

        while (index := await closed.get()) is not None:
            row, fetched = await closing[index]

            if server is not None and fetched is not None:
                row["server"] = server.record(row["window"], fetched)

            rows.append(row)
            writer.write(row)

            # A soak's warm-up is cold start, as a ramp's `warm_s` is: a burst there stops
            # nothing. Only the first stop counts, and a window cut short is already the last.
            if rule or row["warm"] or row.get("cut_short"):
                continue

            stopped = _stop_reason(row, profile.stop_error_rate)

            if stopped is not None:
                rule.append(stopped)
                state.stop(stopped)  # no new sends; the running window ends cut short

    rows_done = asyncio.create_task(write_rows())
    halted_at: int | None = None
    window = 0

    try:
        while True:  # the last window, or a halt, breaks out
            window += 1
            recorder = StepRecorder(warm_until=time.perf_counter())
            state.current = recorder
            state.probe.begin()

            with contextlib.suppress(TimeoutError):
                deadline = started + window * profile.window_s
                await asyncio.wait_for(
                    state.interrupted.wait(), timeout=max(deadline - time.perf_counter(), 0.0)
                )

            check()
            halted = state.halt is not None

            if halted or window == profile.windows:
                halted_at = window if halted else None
                state.stop(_DONE)  # a no-op once halted: either way, nothing more is sent
                state.current = None
                close(window, recorder, cut_short=halted)
                break

            close(window, recorder, cut_short=False)

        closed.put_nowait(None)
        await rows_done
        check()
        halt = state.halt

        if rule:
            return rows, rule[0], ends

        if halted_at is not None and halt is not None:
            return rows, {"window": halted_at, **key, **halt}, ends

        return rows, None, ends

    finally:
        state.current = None
        tasks = [*load, *inflight, *closing, rows_done]

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)


async def _window_row(
    state: _Run,
    server: ServerSeries | None,
    recorder: StepRecorder,
    row: dict[str, Any],
    client: dict[str, Any],
    measured: float,
    *,
    cut_short: bool,
) -> tuple[dict[str, Any], Fetched | None]:
    """Drain a closed window's calls and take its scrape side by side, then finish its row.

    The scrape comes back unrecorded: scrapes can finish out of order, and
    :meth:`ServerSeries.record` takes them in window order.
    """

    drain = recorder.drain(state.plan.timeout_s + _DRAIN_MARGIN_S)
    fetched: Fetched | None = None

    if server is None:
        await drain

    else:
        _, fetched = await asyncio.gather(drain, server.fetch())

    summary = recorder.summary(measured)
    scenarios = summary.pop("scenarios")
    row.update(summary, client=client, scenarios=scenarios)

    if state.exhausted:
        row["sample_exhausted"], state.exhausted = state.exhausted, []

    if cut_short:
        row["cut_short"] = True

    return row, fetched


# ....................... #


def _assignments(principals: Sequence[Principal]) -> Iterator[Principal]:
    """Principals by smooth weighted round robin: their shares hold at every count."""

    total = sum(principal.weight for principal in principals)
    current = [0] * len(principals)

    while True:
        for index, principal in enumerate(principals):
            current[index] += principal.weight

        chosen = max(range(len(principals)), key=current.__getitem__)
        current[chosen] -= total

        yield principals[chosen]


def _cumulative(weights: Mapping[str, float]) -> tuple[list[str], list[float]]:
    return list(weights), list(itertools.accumulate(weights.values()))


def _sample_order(sample: Sample) -> tuple[str, str, int]:
    return sample.operation, sample.field, sample.size


def _step_key(row: Mapping[str, Any]) -> dict[str, Any]:
    return {k: row[k] for k in ("window", "workers", "rate") if k in row}


def _levels(profile: Profile) -> tuple[str, list[float]]:
    """The profile's model and every load level it runs: workers or arrival rates."""

    if isinstance(profile, ClosedRamp):
        return "closed", [float(n) for n in profile.steps]

    if isinstance(profile, OpenRamp):
        return "open", list(profile.rates)

    # A soak has exactly one of the two (`Soak.__attrs_post_init__`).
    if profile.workers is not None:
        return "closed", [float(profile.workers)]

    return "open", [rate for rate in (profile.rate,) if rate is not None]


def _stop_reason(row: Mapping[str, Any], stop_error_rate: float) -> dict[str, Any] | None:
    key = _step_key(row)

    if row["misconfigured"]:
        return {**key, "reason": "misconfigured"}

    if error_share(row) > stop_error_rate:
        return {**key, "reason": "errors"}

    return None


def _meta(plan: LoadPlan, profile: Profile, model: str, catalog: Catalog) -> dict[str, Any]:
    return {
        "kind": "meta",
        "schema": SCHEMA,
        "forze_load": __version__,
        "target": safe_target(plan.base_url),
        "openapi_sha256": catalog.openapi_sha256,
        "model": model,
        "profile": attrs.asdict(profile),
        "plan": {
            "principals": {p.name: p.weight for p in plan.principals},
            "mixes": {p.name: dict(p.mix) for p in plan.principals},
            "scenarios": catalog.described,
            "skipped": catalog.skipped,
            "allow_writes": plan.allow_writes,
            "timeout_s": plan.timeout_s,
            "seed": plan.seed,
        },
        "client": {
            "cpus": os.cpu_count(),
            "python": platform.python_version(),
            "loop": type(asyncio.get_running_loop()).__module__.partition(".")[0],
        },
        "started_at": utcnow().isoformat(),
    }
