"""Cooperative scheduling + simulated I/O latency for deterministic simulation.

Under simulation a port call models real I/O, so at each port boundary the tracing proxy
awaits :func:`cooperative_point`, which (when a simulation has enabled it):

* **yields** to the loop — a real adapter suspends on I/O, so concurrent operations must
  interleave here, not run as if atomic (otherwise interleaving bugs hide); and
* optionally **advances the virtual clock** by a per-port *latency* — a real downstream
  takes wall-clock time, so the simulator advances time for it. This is what lets
  time-dependent bugs (a hold expiring while a slow call runs) surface **without any
  artificial ``await asyncio.sleep(...)`` in application handlers** — the latency is a
  property of the simulated environment, configured test-side, not of the production code.

Both are off by default — a no-op in production and outside simulation.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Callable, Iterator

# ----------------------- #

LatencyModel = Callable[[str | None, str | None, str], float]
"""Map a port call ``(surface, route, op)`` to its simulated latency in seconds."""

_COOPERATIVE: ContextVar[bool] = ContextVar(
    "forze_cooperative_scheduling", default=False
)
_LATENCY: ContextVar[LatencyModel | None] = ContextVar(
    "forze_port_latency", default=None
)


async def cooperative_point(
    surface: str | None = None, route: str | None = None, op: str = ""
) -> None:
    """Yield (and advance the clock by the port's latency) when cooperative scheduling is on."""

    if not _COOPERATIVE.get():
        return

    delay = 0.0
    model = _LATENCY.get()
    if model is not None:
        delay = model(surface, route, op)

    await asyncio.sleep(delay)  # delay 0 → a bare yield; > 0 → advances virtual time


@contextmanager
def cooperative_scheduling(*, latency: LatencyModel | None = None) -> Iterator[None]:
    """Enable cooperative scheduling (port boundaries yield); *latency* models I/O delay."""

    cooperative_token = _COOPERATIVE.set(True)
    latency_token = _LATENCY.set(latency)

    try:
        yield

    finally:
        _LATENCY.reset(latency_token)
        _COOPERATIVE.reset(cooperative_token)
