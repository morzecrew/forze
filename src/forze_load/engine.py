"""The run engine: closed and open load models, driven step by step against a live app."""

from __future__ import annotations

import asyncio
import contextlib
import itertools
import math
import os
import platform
import random
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any, Literal

import attrs
import httpx

from forze.base.exceptions import exc
from forze.base.primitives import utcnow

from ._version import __version__
from .auth import Auth
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

_PROBE_S = 0.05
_BEHIND_S = 0.01


# ....................... #


def _ints(values: Iterable[int]) -> tuple[int, ...]:
    return tuple(values)


def _floats(values: Iterable[float]) -> tuple[float, ...]:
    return tuple(values)


def _positive_steps(values: Sequence[float], what: str) -> None:
    if not values or any(value <= 0 for value in values):
        raise exc.configuration(f"{what} must be a non-empty list of positive numbers")


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
        if not 0.0 <= self.warm_s < self.step_s:
            raise exc.configuration("warm_s must be at least 0 and below step_s")


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


@attrs.frozen(kw_only=True)
class OpenRamp(_Ramp):
    """Open model: arrivals at a rate per step, whatever the answers do.

    Latency runs from each request's intended send time, so a stall shows in every request
    scheduled during it. In-flight requests are capped at rate × request timeout; a send
    refused at the cap counts as ``transport``.
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
    ends, so an interrupted run keeps the steps it finished.

    :param plan: Target, principals and scenarios.
    :param profile: :class:`ClosedRamp` or :class:`OpenRamp`.
    :param out: JSON lines file to write, or ``None`` to only return the result.
    :param transport: An ``httpx`` transport to use instead of the network (tests).
    :returns: The meta record, the rows and the summary.
    :raises CoreException: ``configuration`` — the plan is refused (:func:`validate_plan`),
        a credential is missing, or a sign-in fails.
    """

    validate_plan(plan)
    model = "closed" if isinstance(profile, ClosedRamp) else "open"
    connections = (
        max(profile.steps)
        if isinstance(profile, ClosedRamp)
        else max(_cap(rate, plan.timeout_s) for rate in profile.rates)
    )

    async with httpx.AsyncClient(
        base_url=plan.base_url,
        timeout=plan.timeout_s,
        # httpx's default pool is 100 connections: above that, requests would queue in the
        # client and be measured as server latency.
        limits=httpx.Limits(max_connections=connections, max_keepalive_connections=connections),
        transport=transport,
    ) as client:
        state = _Run(plan=plan, client=client)
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
        # The loop thread's CPU, not the process's: one event loop runs Python on one core,
        # and an app served in the same process (a test) must not count as the client.
        self._thread = time.thread_time()
        self._process = time.process_time()

    def report(self, scheduled: int, behind: int) -> dict[str, Any]:
        wall = max(time.perf_counter() - self._wall, 1e-9)
        loop_cpu = (time.thread_time() - self._thread) / wall
        lags = sorted(self.lags)
        lag_p99 = lags[min(len(lags) - 1, int(0.99 * len(lags)))] * 1000 if lags else 0.0
        behind_share = behind / scheduled if scheduled else 0.0

        return {
            "loop_cpu": round(loop_cpu, 3),
            "process_cpu": round((time.process_time() - self._process) / wall, 3),
            "loop_lag_p99_ms": round(lag_p99, 2),
            "behind_schedule": behind,
            "saturated": loop_cpu >= SATURATED_CPU
            or lag_p99 > SATURATED_LAG_MS
            or behind_share > SATURATED_BEHIND,
        }


# ....................... #


@attrs.define
class _Session:
    """One principal's credential, shared by every worker that runs as it."""

    principal: Principal
    client: httpx.AsyncClient

    async def headers(self) -> dict[str, str]:
        auth = self.principal.auth
        headers: dict[str, str] = {}

        if isinstance(auth, Auth):
            headers.update(await auth.headers(self.client))

        elif auth is not None:
            headers.update(await auth(self.client))

        if self.principal.tenant is not None:
            headers[TENANT_HEADER] = self.principal.tenant

        return headers


@attrs.define
class _Run:
    plan: LoadPlan
    client: httpx.AsyncClient
    probe: _Probe = attrs.field(factory=_Probe)
    recorder: StepRecorder = attrs.field(factory=StepRecorder)
    warm_until: float = 0.0
    failure: BaseException | None = None
    failed: asyncio.Event = attrs.field(factory=asyncio.Event)
    rng: random.Random = attrs.field(init=False)
    sessions: dict[str, _Session] = attrs.field(init=False)
    scenarios: dict[str, Scenario] = attrs.field(init=False)
    mixes: dict[str, tuple[list[str], list[int]]] = attrs.field(init=False)

    def __attrs_post_init__(self) -> None:
        self.rng = random.Random(self.plan.seed)  # nosec B311 - seeded load picks, not crypto
        self.sessions = {p.name: _Session(p, self.client) for p in self.plan.principals}
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

    def record(self, scenario: str, outcome: str, latency_ms: float | None) -> None:
        if time.perf_counter() >= self.warm_until:
            self.recorder.record(scenario, outcome, latency_ms)

    def fail(self, error: BaseException) -> None:
        if self.failure is None:
            self.failure = error
            self.failed.set()

    # ....................... #

    async def call(self, principal: Principal, started: float) -> None:
        """Send one request as *principal*; latency runs from *started*."""

        name = self.pick(principal)

        try:
            headers = await self.sessions[principal.name].headers()
            call = Call(client=self.client, headers=headers, principal=principal.name, rng=self.rng)
            outcome = classify((await self.scenarios[name].send(call)).status_code)

        except httpx.TransportError:  # timeouts included
            outcome = "transport"

        self.record(name, outcome, (time.perf_counter() - started) * 1000)

    async def guarded(self, principal: Principal, started: float) -> None:
        try:
            await self.call(principal, started)

        except Exception as error:  # a scenario's own bug stops the run, loudly
            self.fail(error)

    async def worker(self, principal: Principal, think_s: float) -> None:
        while self.failure is None:
            await self.guarded(principal, time.perf_counter())
            # Always yield: a scenario that answers without suspending (a cached response, a
            # fake transport) would otherwise hold the loop and starve the step's timer.
            await asyncio.sleep(think_s)

    async def arrivals(
        self,
        rate: float,
        kind: str,
        cap: int,
        inflight: set[asyncio.Task[None]],
        assign: Iterator[Principal],
    ) -> None:
        start = due = time.perf_counter()
        sent = 0

        while True:
            due = due + self.rng.expovariate(rate) if kind == "poisson" else start + sent / rate
            sent += 1
            delay = due - time.perf_counter()

            # Yields even when behind, so the sends already started get to run.
            await asyncio.sleep(max(delay, 0.0))

            now = time.perf_counter()

            if now >= self.warm_until:
                self.recorder.scheduled += 1
                self.recorder.behind += int(now - due > _BEHIND_S)

            principal = next(assign)

            if len(inflight) >= cap:
                self.record(self.pick(principal), "transport", None)
                continue

            task = asyncio.create_task(self.guarded(principal, started=due))
            inflight.add(task)
            task.add_done_callback(inflight.discard)

    # ....................... #

    async def step(
        self,
        profile: _Ramp,
        key: Mapping[str, Any],
        latency: str,
    ) -> dict[str, Any]:
        """Run one step's window and return its row; warm-up is left out of the numbers."""

        self.recorder = StepRecorder()
        self.warm_until = time.perf_counter() + profile.warm_s
        await self._sleep(profile.warm_s)
        self.probe.begin()
        await self._sleep(profile.step_s - profile.warm_s)
        measured = profile.step_s - profile.warm_s
        summary = self.recorder.summary(measured)
        scenarios = summary.pop("scenarios")

        return {
            "kind": "step",
            **key,
            "latency": latency,
            **summary,
            "client": self.probe.report(self.recorder.scheduled, self.recorder.behind),
            "scenarios": scenarios,
        }

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self.failed.wait(), timeout=seconds)

        if self.failure is not None:
            raise self.failure


# ....................... #


async def _closed(
    state: _Run,
    profile: ClosedRamp,
    writer: RunWriter,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    workers: list[asyncio.Task[None]] = []
    assign = _assignments(state.plan.principals)
    rows: list[dict[str, Any]] = []
    stopped: dict[str, Any] | None = None

    try:
        for n in profile.steps:
            while len(workers) < n:
                workers.append(asyncio.create_task(state.worker(next(assign), profile.think_s)))

            while len(workers) > n:
                workers.pop().cancel()

            row = await state.step(profile, {"workers": n}, "closed")
            rows.append(row)
            writer.write(row)
            stopped = _stop_reason(row, profile.stop_error_rate)

            if stopped is not None:
                break

    finally:
        for task in workers:
            task.cancel()

        await asyncio.gather(*workers, return_exceptions=True)

    return rows, stopped


async def _open(
    state: _Run,
    profile: OpenRamp,
    writer: RunWriter,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    inflight: set[asyncio.Task[None]] = set()
    assign = _assignments(state.plan.principals)
    rows: list[dict[str, Any]] = []
    stopped: dict[str, Any] | None = None

    try:
        for rate in profile.rates:
            cap = _cap(rate, state.plan.timeout_s)
            driver = asyncio.create_task(
                state.arrivals(rate, profile.arrivals, cap, inflight, assign)
            )

            try:
                row = await state.step(profile, {"rate": rate}, "open")

            finally:
                driver.cancel()
                await asyncio.gather(driver, return_exceptions=True)

            rows.append(row)
            writer.write(row)
            stopped = _stop_reason(row, profile.stop_error_rate)

            if stopped is not None:
                break

        if inflight:
            await asyncio.wait(inflight, timeout=state.plan.timeout_s)

    finally:
        for task in inflight:
            task.cancel()

        await asyncio.gather(*inflight, return_exceptions=True)

    return rows, stopped


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


def _cap(rate: float, timeout_s: float) -> int:
    return max(1, math.ceil(rate * timeout_s))


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
