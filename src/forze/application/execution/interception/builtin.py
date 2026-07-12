"""Built-in port interceptors for deterministic simulation."""

import asyncio
from contextlib import aclosing
from typing import Any, AsyncGenerator, AsyncIterator, Callable, cast

import attrs

from .protocol import PortCall, PortNext, StreamPortNext

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

    # ....................... #

    async def around_stream(
        self, call: PortCall, nxt: StreamPortNext
    ) -> AsyncIterator[Any]:
        """Yield at stream open *and before each item*, so a streamed read is an interleaving
        point per item — not just once at acquisition. Without this, two concurrent operations
        consuming a stream run as if each item batch were atomic and interleaving bugs in
        stream consumption hide. The optional latency models the stream-open cost once (a
        per-item latency would over-advance virtual time for a long stream)."""

        delay = 0.0

        if self.latency is not None:
            delay = self.latency(call.surface, call.route, call.op)

        await asyncio.sleep(delay)  # stream-open cost + a yield

        # ``aclosing`` closes the inner stream deterministically on any exit (consumer
        # ``aclose``, early break, a thrown-in exception) — a backend cursor is released
        # at scope exit, not whenever GC finalizes an abandoned generator.
        async with aclosing(cast("AsyncGenerator[Any, None]", nxt(call))) as stream:
            async for item in stream:
                await asyncio.sleep(0)  # each item is an interleaving point
                yield item
