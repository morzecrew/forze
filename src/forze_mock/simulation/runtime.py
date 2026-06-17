"""Run an async scenario under deterministic virtual time + seeded entropy.

``run_simulation`` is the one-call entry point: it spins up a
:class:`SimulationEventLoop`, binds a :class:`SimulationTimeSource` and a
:class:`SeededEntropySource` for the duration, and drives the scenario to
completion. Same ``(scenario, seed)`` → identical execution, in real-wall
milliseconds however much virtual time the scenario spans.
"""

from datetime import datetime
from typing import Awaitable, Callable

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
) -> T:
    """Run *scenario* on a deterministic virtual-time loop with seeded entropy.

    *scenario* is a zero-argument coroutine function (call it to get the coroutine).
    The simulation time + entropy seams are bound for the run, so every ``utcnow``,
    ``monotonic``, ``uuid7``/``uuid4``, jitter, and nonce read is a pure function of
    ``(seed, epoch)`` and the scenario's own sleeps. Returns the scenario's result.

    Raises :class:`~forze_mock.simulation.loop.SimulationDeadlock` if the scenario
    blocks with no pending timer, or :class:`~forze_mock.simulation.loop.RealIOForbidden`
    if it touches real I/O or a thread executor.
    """

    loop = SimulationEventLoop()
    time_source = SimulationTimeSource(loop=loop, epoch=epoch)
    entropy = SeededEntropySource(seed=seed)

    try:
        with bind_time_source(time_source), bind_entropy_source(entropy):
            return loop.run_until_complete(scenario())

    finally:
        loop.close()
