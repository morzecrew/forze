"""Run an async scenario under deterministic virtual time + seeded entropy.

``run_simulation`` is the one-call entry point: it spins up a
:class:`SimulationEventLoop`, binds a :class:`SimulationTimeSource` and a
:class:`SeededEntropySource` for the duration, and drives the scenario to
completion. Same ``(scenario, seed)`` → identical execution, in real-wall
milliseconds however much virtual time the scenario spans.
"""

import random
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from typing import final

import attrs

from forze.application.execution.interception import (
    CooperativeInterceptor,
    LatencyModel,
    bind_interceptors,
)
from forze.base.primitives import (
    SeededEntropySource,
    bind_cpu_executor,
    bind_entropy_source,
    bind_time_source,
)

from .cpu import CpuCostModel, SimulationCpuExecutor
from .loop import RealIOForbidden, ScheduleProfile, SimulationDeadlock, SimulationEventLoop
from .scheduler import Reorderer
from .time_source import DEFAULT_EPOCH, SimulationTimeSource

# ----------------------- #


@final
@attrs.define(kw_only=True)
class ScheduleProfiler:
    """Accumulates measured schedule profiles across runs — the maxima the PCT bound needs.

    The bound ``p ≥ 1/(n · k^(d-1))`` holds per run; folding a campaign's runs with the maxima
    keeps the comparison conservative (the largest observed contention gives the lowest floor).
    A "run" here is one event loop's lifetime — a crash → restart trial contributes one profile
    per segment, which the max-fold absorbs.
    """

    max_tasks: int = 0
    max_choice_steps: int = 0
    runs: int = 0

    # ....................... #

    def observe(self, profile: ScheduleProfile) -> None:
        self.max_tasks = max(self.max_tasks, profile.tasks)
        self.max_choice_steps = max(self.max_choice_steps, profile.choice_steps)
        self.runs += 1


# ....................... #

_schedule_profiler: ContextVar[ScheduleProfiler | None] = ContextVar(
    "forze_dst_schedule_profiler", default=None
)


@contextmanager
def profile_schedules(profiler: ScheduleProfiler | None = None) -> Iterator[ScheduleProfiler]:
    """Collect every :func:`run_simulation` schedule profile in scope into *profiler*.

    Profiling is off unless a collector is in scope, so the loop's per-tick bookkeeping costs
    nothing by default.
    """

    collector = profiler if profiler is not None else ScheduleProfiler()
    token = _schedule_profiler.set(collector)
    try:
        yield collector
    finally:
        _schedule_profiler.reset(token)


# ....................... #


def run_simulation[T](
    scenario: Callable[[], Awaitable[T]],
    *,
    seed: int = 0,
    epoch: datetime = DEFAULT_EPOCH,
    schedule_seed: int | None = None,
    scheduler: Reorderer | None = None,
    latency: LatencyModel | None = None,
    cpu_cost: CpuCostModel | None = None,
) -> T:
    """Run *scenario* on a deterministic virtual-time loop with seeded entropy.

    *scenario* is a zero-argument coroutine function (call it to get the coroutine).
    The simulation time + entropy seams are bound for the run, so every ``utcnow``,
    ``monotonic``, ``uuid7``/``uuid4``, jitter, and nonce read is a pure function of
    ``(seed, epoch)`` and the scenario's own sleeps. Returns the scenario's result.

    Interleaving control (opt-in): pass *schedule_seed* to shuffle the ready-callback queue
    each tick from a separate seeded RNG, or *scheduler* for a custom strategy (e.g. a
    :class:`~forze_dst.scheduler.PCTReorderer`) — *scheduler* takes precedence. ``None`` for
    both keeps deterministic FIFO order. Either way the run is reproducible.

    *latency* models simulated I/O delay: a ``(surface, route, op) -> seconds`` function called
    at each port boundary to advance the virtual clock (a real downstream takes time). Lets
    time-dependent bugs surface without artificial sleeps in handlers. ``None`` = instant ports.

    *cpu_cost* is the analogue for off-loop CPU work: a ``(label) -> seconds`` function (the
    label is the offloaded callable's qualname) that advances the virtual clock for a
    ``run_cpu`` / ``run_cpu_map`` call. Under simulation that work runs **inline** (so it is
    deterministic and never trips ``RealIOForbidden``), and each call is a yield point a
    deadline can fire "during". ``None`` = instant offloads.

    Raises :class:`~forze_dst.loop.SimulationDeadlock` if the scenario
    blocks with no pending timer, or :class:`~forze_dst.loop.RealIOForbidden`
    if it touches real I/O or a *raw* thread executor (use ``run_cpu`` instead).
    """

    if epoch.tzinfo is None:
        # A naive epoch's ``.timestamp()`` is interpreted in the host's local timezone, so
        # ``now()`` / ``uuid7`` derivation would vary by machine — defeating reproducibility.
        raise ValueError("epoch must be timezone-aware (e.g. tzinfo=UTC)")

    schedule_rng = (
        None if schedule_seed is None else random.Random(schedule_seed)  # nosec B311 - deterministic sim schedule, not crypto
    )
    profiler = _schedule_profiler.get()
    loop = SimulationEventLoop(
        schedule_rng=schedule_rng, scheduler=scheduler, profile_schedule=profiler is not None
    )
    time_source = SimulationTimeSource(loop=loop, epoch=epoch)
    entropy = SeededEntropySource(seed=seed)

    try:
        # The cooperative interceptor makes every port call a yield point, so concurrent
        # operations interleave at port boundaries (real adapters suspend on I/O; the mocks
        # don't) — the scheduler then explores those interleavings. Bound run-scoped (ambient)
        # since run_simulation does not own the deps registry. No app code required.
        with (
            bind_time_source(time_source),
            # Only the *replayable* seam is seeded: jitter, sampling, and random ids reproduce from
            # the seed. Durable secrets (nonces/tokens/keys) read the separate SecretEntropy seam,
            # which stays CSPRNG here — a seeded source is a different type that cannot serve them,
            # so a predictable secret is unrepresentable rather than gated by an opt-in flag.
            bind_entropy_source(entropy),
            bind_cpu_executor(SimulationCpuExecutor(cost=cpu_cost)),
            bind_interceptors(CooperativeInterceptor(latency=latency)),
        ):
            return loop.run_until_complete(scenario())

    finally:
        # Observed in the finally so violating and crashing runs are profiled too — the
        # killing run's measured n / k are exactly the ones the bound comparison needs.
        if profiler is not None:
            profiler.observe(loop.schedule_profile())
        loop.close()


# ....................... #

# ``runtime`` is the low-level deterministic-runtime namespace: the one-call entry plus the
# loop, its leak/deadlock guards, and the virtual-time clock seam, re-exported together.
__all__ = [
    "run_simulation",
    "ScheduleProfile",
    "ScheduleProfiler",
    "profile_schedules",
    "SimulationEventLoop",
    "RealIOForbidden",
    "SimulationDeadlock",
    "SimulationTimeSource",
    "SimulationCpuExecutor",
    "CpuCostModel",
    "DEFAULT_EPOCH",
]
