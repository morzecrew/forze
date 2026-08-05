"""A liveness/readiness surface for processes that are not an HTTP server.

``attach_readiness_route`` lives in ``forze_fastapi``, which means the processes most
likely to wedge silently — the outbox relay, inbox and commit-stream consumers, the
socket.io gateway, durable runners — expose nothing a Kubernetes probe can hit. Their
health is today only *inferable*, from gauges like ``forze.realtime.backplane
.seconds_since_ok``, and a gauge is an alerting signal, not a restart signal.

This is the smallest thing that closes that gap: one ``asyncio`` server, a hand-rolled
HTTP/1.0 responder, and the same ready/draining/unavailable mapping the FastAPI route
uses, read from the same runtime state. **Stdlib only, deliberately** — a utility
container must not grow FastAPI or aiohttp to answer two endpoints, and there is nothing
here worth the dependency: no routing, no bodies, no keep-alive.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from typing import TYPE_CHECKING, Final, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from .loop import DEFAULT_STOP_GRACE_SECONDS

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext
    from forze.application.execution.runtime import ExecutionRuntime

# ----------------------- #

__all__ = [
    "PROBE_LISTENER_STEP_ID",
    "probe_listener_step",
]

PROBE_LISTENER_STEP_ID: Final[str] = "probe_listener"

_REQUEST_LINE_LIMIT: Final[int] = 8_192
"""Cap on the request line. A probe sends ``GET /readyz HTTP/1.1``; anything longer is
either a mistake or someone poking the port, and neither earns unbounded buffering."""

_READ_TIMEOUT: Final[float] = 5.0
"""A connection that opens and says nothing is dropped rather than held. Without this a
handful of idle sockets — a port scanner, a half-open load-balancer check — would pin
handler tasks for the life of the process."""

_STATUS_TEXT: Final[dict[int, str]] = {
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    503: "Service Unavailable",
}


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _ProbeListenerStartup(LifecycleHook):
    """Serve the two probe paths for as long as the process should answer them."""

    runtime: ExecutionRuntime
    host: str
    port: int
    path_ready: str
    path_live: str

    server: asyncio.Server | None = attrs.field(default=None, init=False, repr=False)

    connections: set[asyncio.StreamWriter] = attrs.field(factory=set, init=False, repr=False)
    """Live client transports, so shutdown can hang up on them.

    ``Server.wait_closed()`` waits for every handler to finish, and a handler sitting in
    ``readline`` waits out the full read timeout. Without this, one idle socket — a port
    scanner, a half-open load-balancer check — would hold the graceful stop open past the
    drain budget and log a "did not close" warning about a listener that had no work left.
    """

    # ....................... #

    @property
    def loop_name(self) -> str:
        """Satisfies ``DrainableLoop``."""

        return f"probe_listener:{self.port}"

    # ....................... #

    async def stop(self, *, deadline: float) -> bool:
        """Stop answering probes and close the socket. Idempotent.

        Registered as a drainable, so this runs *after* the drain gate flipped: the probe
        reports ``draining`` for the whole drain window — which is what makes a rolling
        update take the pod out of rotation instead of killing it mid-drain — and only then
        does the endpoint disappear.
        """

        server = self.server

        if server is None:
            return True

        self.server = None
        server.close()

        # Closing the listening socket only stops *new* connections. Existing ones are
        # hung up on here: a probe response is a single write with no state behind it, so
        # there is nothing in flight worth waiting for.
        for writer in tuple(self.connections):
            writer.close()

        clock = asyncio.get_running_loop()
        budget = max(0.0, deadline - clock.time())

        try:
            await asyncio.wait_for(asyncio.shield(server.wait_closed()), timeout=budget)

        except TimeoutError:
            logger.warning(
                "Probe listener on port %d did not close within %.1fs", self.port, budget
            )

            return False

        return True

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        if self.server is not None:
            return

        self.server = await asyncio.start_server(self._handle, host=self.host, port=self.port)
        ctx.drainables.register(self)

        logger.info(
            "Probe listener serving %s and %s on port %d",
            self.path_live,
            self.path_ready,
            self.port,
        )

    # ....................... #

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.connections.add(writer)

        try:
            method, (status, payload) = await self._answer(reader)
            # HEAD carries the headers of the equivalent GET and none of its body.
            writer.write(_response(status, payload, body=method != "HEAD"))
            await writer.drain()

        except (TimeoutError, ConnectionError, asyncio.IncompleteReadError):
            # A probe client that hangs up mid-request is routine (kubelet timeouts,
            # port scans); it must never surface as an error in a worker's logs.
            pass

        except Exception:
            logger.warning("Probe listener request failed", exc_info=True)

        finally:
            self.connections.discard(writer)
            writer.close()

            with suppress(ConnectionError, OSError):
                await writer.wait_closed()

    # ....................... #

    async def _answer(self, reader: asyncio.StreamReader) -> tuple[str, tuple[int, str]]:
        """The request's method, and the status/payload to answer it with."""

        try:
            line = await asyncio.wait_for(reader.readline(), timeout=_READ_TIMEOUT)

        except ValueError:
            # The stream's own buffer limit (64 KiB) overflowed before our cap could
            # apply. Still just a malformed request — answering 400 keeps a port scanner
            # from writing a stack trace into a worker's logs on every connection.
            return "GET", (400, "bad_request")

        if not line or len(line) > _REQUEST_LINE_LIMIT:
            return "GET", (400, "bad_request")

        parts = line.decode("latin-1", errors="replace").split()

        if len(parts) < 2 or parts[0] not in ("GET", "HEAD"):
            return "GET", (400, "bad_request")

        method = parts[0]
        path = parts[1].split("?", 1)[0]

        if path == self.path_live:
            # Same contract as the FastAPI route: reaching here *is* the check.
            return method, (200, "alive")

        if path != self.path_ready:
            return method, (404, "not_found")

        if self.runtime.ready:
            return method, (200, "ready")

        return method, (503, "draining" if self.runtime.draining else "unavailable")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _ProbeListenerShutdown(LifecycleHook):
    """Close the listener.

    Normally a no-op — the runtime stops every registered drainable before teardown. This
    is the fallback for a hand-driven lifecycle; ``stop`` is idempotent.
    """

    startup: _ProbeListenerStartup

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        clock = asyncio.get_running_loop()
        await self.startup.stop(deadline=clock.time() + DEFAULT_STOP_GRACE_SECONDS)


# ....................... #


def _response(status: int, payload: str, *, body: bool = True) -> bytes:
    content = f'{{"status":"{payload}"}}'.encode()
    head = (
        f"HTTP/1.0 {status} {_STATUS_TEXT[status]}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(content)}\r\n"
        f"Cache-Control: no-store\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()

    return head + content if body else head


# ----------------------- #


def probe_listener_step(
    runtime: ExecutionRuntime,
    *,
    host: str = "0.0.0.0",  # nosec B104 - a kubelet probe reaches the pod from outside it
    port: int,
    path_ready: str = "/readyz",
    path_live: str = "/livez",
    step_id: StrKey = PROBE_LISTENER_STEP_ID,
) -> LifecycleStep:
    """Build a lifecycle step serving ``/livez`` and ``/readyz`` for a non-HTTP process.

    Wire it into any runner's lifecycle — outbox relay, queue or stream consumer, socket.io
    gateway, durable runner — and point Kubernetes at it: liveness on *path_live*,
    readiness on *path_ready*, startup on *path_ready* with a generous failure threshold.

    Readiness answers from the same runtime state as the FastAPI route: ``ready`` while a
    scope is active and not draining, ``draining`` for the whole shutdown window,
    ``unavailable`` before the scope exists. Liveness answers ``200`` unconditionally,
    because a process whose event loop still schedules this handler is by definition alive.

    Probes remain the *restart* signal, not the alerting one: keep alerting on the gauges
    (backplane freshness, mailbox overflow, job staleness), which detect a loop that is
    running but making no progress — something no probe can see.

    :param host: Bind address. Defaults to all interfaces, because the probe request comes
        from the node, not from inside the container.
    :param port: Bind port; pick one per process (8079 by convention for workers).

    :raises CoreException: ``configuration`` — the port is out of range, or the two probe
        paths are the same.
    """

    if not 1 <= port <= 65_535:
        raise exc.configuration(f"Probe listener port must be in 1..65535, got {port}")

    if path_ready == path_live:
        raise exc.configuration(
            "Probe listener paths must differ: a single path cannot answer both liveness "
            "and readiness without making a draining pod look dead"
        )

    startup = _ProbeListenerStartup(
        runtime=runtime,
        host=host,
        port=port,
        path_ready=path_ready,
        path_live=path_live,
    )

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_ProbeListenerShutdown(startup=startup),
        requires_long_running=True,
    )
