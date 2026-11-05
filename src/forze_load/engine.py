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
from .plan import TENANT_HEADER, Call, LoadPlan, Principal, Scenario, safe_target, validate_plan
from .report import SCHEMA, RunWriter
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


type Profile = ClosedRamp | OpenRamp


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
) -> RunResult:
    """Drive *plan* against its target with *profile*, and write JSON lines to *out*.

    The plan is validated first, so a refused write or a bad mix stops the run before
    anything is sent. Principals are then signed in, at most :data:`LOGIN_CONCURRENCY` at
    once, and the first failure stops the run before any load. Rows are written as each step
    ends, so an interrupted run keeps the steps it finished. A ramp that stops on its stop
    rule is a finished run: the summary says where and why.

    :param plan: Target, principals and scenarios.
    :param profile: :class:`ClosedRamp` or :class:`OpenRamp`.
    :param out: JSON lines file to write, or ``None`` to only return the result.
    :param transport: An ``httpx`` transport to use instead of the network. An app served
        through it on the same event loop runs on the loop's thread, so the loop's CPU is
        then reported but not judged (``loop_cpu_judged: false``).
    :returns: The meta record, the rows and the summary.
    :raises CoreException: ``configuration`` — the plan is refused (:func:`validate_plan`),
        a credential is missing, a sign-in fails, a refresh fails after the token expired,
        or a scenario passes :meth:`Call.request` a URL with a host.
    """

    validate_plan(plan)
    model = "closed" if isinstance(profile, ClosedRamp) else "open"
    connections = (
        max(profile.steps)
        if isinstance(profile, ClosedRamp)
        else max(inflight_cap(rate, plan.timeout_s) for rate in profile.rates)
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
        state = _Run(
            plan=plan,
            client=client,
            auth_client=auth_client,
            cpu_judged=transport is None,
        )
        meta = _meta(plan, profile, model)

        with RunWriter(out) as writer:
            writer.write(meta)
            await state.sign_in()
            probe = asyncio.create_task(state.probe.run())

            try:
                if isinstance(profile, ClosedRamp):
                    rows, stopped = await _closed(state, profile, writer)

                else:
                    rows, stopped = await _open(state, profile, writer)

            finally:
                probe.cancel()
                await asyncio.gather(probe, return_exceptions=True)

            knee = find_knee(rows, model=model)
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
    client: httpx.AsyncClient
    auth_client: httpx.AsyncClient
    cpu_judged: bool
    probe: _Probe = attrs.field(factory=_Probe)
    current: StepRecorder | None = None
    failure: BaseException | None = None
    failed: asyncio.Event = attrs.field(factory=asyncio.Event)
    sessions: dict[str, _Session] = attrs.field(init=False)
    scenarios: dict[str, Scenario] = attrs.field(init=False)
    mixes: dict[str, tuple[list[str], list[int]]] = attrs.field(init=False)
    rng: random.Random = attrs.field(init=False)
    arrival_rng: random.Random = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        # Picks and `Call.rng` share a stream; Poisson arrivals draw from their own, so a
        # scenario that draws more or fewer numbers does not move the arrival times.
        self.rng = seeded_random(self.plan.seed, "picks")
        self.arrival_rng = seeded_random(self.plan.seed, "arrivals")
        self.sessions = {p.name: _Session(p, self.auth_client) for p in self.plan.principals}
        self.scenarios = {s.name: s for s in self.plan.scenarios}
        self.mixes = {
            p.name: (list(p.mix), list(itertools.accumulate(p.mix.values())))
            for p in self.plan.principals
        }

    # ....................... #

    async def sign_in(self) -> None:
        sessions = list(self.sessions.values())

        for start in range(0, len(sessions), LOGIN_CONCURRENCY):
            batch = sessions[start : start + LOGIN_CONCURRENCY]
            await asyncio.gather(*(session.headers() for session in batch))

    def pick(self, principal: Principal) -> str:
        names, cumulative = self.mixes[principal.name]

        return self.rng.choices(names, cum_weights=cumulative)[0]

    def measuring(self) -> StepRecorder | None:
        """The step a call sent now counts in, or ``None`` outside a measured window."""

        step = self.current

        return step if step is not None and time.perf_counter() >= step.warm_until else None

    def fail(self, error: BaseException) -> None:
        if self.failure is None:
            self.failure = error
            self.failed.set()

    # ....................... #

    async def call(self, principal: Principal, started: float, step: StepRecorder | None) -> None:
        """Send one request as *principal*; latency runs from *started*, auth time excluded.

        *step* is the step current when the call was sent (``None`` outside a measured
        window), so a slow answer counts in the step that sent it. A call that got no answer
        (a transport failure) is counted but has no latency: percentiles cover answered
        calls.
        """

        name = self.pick(principal)

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
        while self.failure is None:
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
            step = self.measuring()

            if step is not None:
                step.scheduled += 1
                step.behind += int(time.perf_counter() - due > _BEHIND_S)

            principal = next(assign)

            if len(inflight) >= cap:
                if step is not None:
                    step.record(self.pick(principal), "transport", None)

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
        the measured window's real length.
        """

        opened = time.perf_counter()
        step = StepRecorder(warm_until=opened + profile.warm_s)
        self.current = step
        await self._sleep_until(step.warm_until)
        self.probe.begin()
        await self._sleep_until(opened + profile.step_s)
        client = self.probe.report(step.scheduled, step.behind, cpu_judged=self.cpu_judged)
        measured = time.perf_counter() - step.warm_until
        self.current = None
        await step.drain(self.plan.timeout_s + _DRAIN_MARGIN_S)

        if self.failure is not None:
            raise self.failure

        summary = step.summary(measured)
        scenarios = summary.pop("scenarios")

        return {
            "kind": "step",
            **key,
            "latency": latency,
            **summary,
            "client": client,
            "scenarios": scenarios,
        }

    async def _sleep_until(self, deadline: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(
                self.failed.wait(), timeout=max(deadline - time.perf_counter(), 0.0)
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


def _stop_reason(row: Mapping[str, Any], stop_error_rate: float) -> dict[str, Any] | None:
    key = {k: row[k] for k in ("workers", "rate") if k in row}

    if row["misconfigured"]:
        return {**key, "reason": "misconfigured"}

    if error_share(row) > stop_error_rate:
        return {**key, "reason": "errors"}

    return None


def _meta(plan: LoadPlan, profile: Profile, model: str) -> dict[str, Any]:
    return {
        "kind": "meta",
        "schema": SCHEMA,
        "forze_load": __version__,
        "target": safe_target(plan.base_url),
        "model": model,
        "profile": attrs.asdict(profile),
        "plan": {
            "principals": {p.name: p.weight for p in plan.principals},
            "mixes": {p.name: dict(p.mix) for p in plan.principals},
            "scenarios": {s.name: {"writes": s.writes} for s in plan.scenarios},
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
