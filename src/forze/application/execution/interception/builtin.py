"""Built-in port interceptors for deterministic simulation."""

import asyncio
from typing import Any, Callable

import attrs

from .protocol import PortCall, PortNext

# ----------------------- #

LatencyModel = Callable[[str | None, str | None, str], float]
"""Map a port call ``(surface, route, op)`` to its simulated latency in seconds."""


# ....................... #


@attrs.define(slots=True, frozen=True)
class CooperativeInterceptor:
    """Yield at the port boundary and advance the virtual clock by the call's latency.

    Under simulation the in-memory adapters do not suspend on I/O, so without a yield two
    concurrent operations run as if atomic and interleaving bugs hide; awaiting here makes
    every port call an interleaving point the scheduler can explore. The optional *latency*
    model advances the virtual clock for a slow downstream — modeling time at the
    simulated-I/O boundary, never with an artificial ``sleep`` in application handlers.
    """

    latency: LatencyModel | None = None
    """The latency model to use."""

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> Any:
        delay = 0.0

        if self.latency is not None:
            delay = self.latency(call.surface, call.route, call.op)

        await asyncio.sleep(
            delay
        )  # delay 0 -> a bare yield; > 0 -> advances virtual time

        return await nxt(call)
