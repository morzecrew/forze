"""Run an async scenario under deterministic virtual time + seeded entropy.

``run_simulation`` is the one-call entry point: it spins up a
:class:`SimulationEventLoop`, binds a :class:`SimulationTimeSource` and a
:class:`SeededEntropySource` for the duration, and drives the scenario to
completion. Same ``(scenario, seed)`` → identical execution, in real-wall
milliseconds however much virtual time the scenario spans.
"""

import random
from datetime import datetime
from typing import Awaitable, Callable

from forze.application.execution.tracing.cooperative import cooperative_scheduling
from forze.base.primitives import (
    SeededEntropySource,
    bind_entropy_source,
    bind_time_source,
)

from .loop import SimulationEventLoop
from .time_source import DEFAULT_EPOCH, SimulationTimeSource

# ----------------------- #


def run_simulation[T](
    scenario: Callable[[], Awaitable[T]],
    *,
    seed: int = 0,
    epoch: datetime = DEFAULT_EPOCH,
    schedule_seed: int | None = None,
    scheduler: object | None = None,
) -> T:
    """Run *scenario* on a deterministic virtual-time loop with seeded entropy.

    *scenario* is a zero-argument coroutine function (call it to get the coroutine).
    The simulation time + entropy seams are bound for the run, so every ``utcnow``,
    ``monotonic``, ``uuid7``/``uuid4``, jitter, and nonce read is a pure function of
    ``(seed, epoch)`` and the scenario's own sleeps. Returns the scenario's result.

    Interleaving control (opt-in): pass *schedule_seed* to shuffle the ready-callback queue
    each tick from a separate seeded RNG, or *scheduler* for a custom strategy (e.g. a
    :class:`~forze_dst.scheduler.PCTScheduler`) — *scheduler* takes precedence. ``None`` for
    both keeps deterministic FIFO order. Either way the run is reproducible.

    Raises :class:`~forze_dst.loop.SimulationDeadlock` if the scenario
    blocks with no pending timer, or :class:`~forze_dst.loop.RealIOForbidden`
    if it touches real I/O or a thread executor.
    """

    schedule_rng = (
        None
        if schedule_seed is None
        else random.Random(schedule_seed)  # nosec B311 - deterministic sim schedule, not crypto
    )
    loop = SimulationEventLoop(schedule_rng=schedule_rng, scheduler=scheduler)
    time_source = SimulationTimeSource(loop=loop, epoch=epoch)
    entropy = SeededEntropySource(seed=seed)

    try:
        # Cooperative scheduling makes every traced port call a yield point, so concurrent
        # operations interleave at port boundaries (real adapters suspend on I/O; the mocks
        # don't) — the scheduler then explores those interleavings. No app code required.
        with (
            bind_time_source(time_source),
            bind_entropy_source(entropy),
            cooperative_scheduling(),
        ):
            return loop.run_until_complete(scenario())

    finally:
        loop.close()
