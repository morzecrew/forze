"""A liveness/readiness surface for processes that are not an HTTP server.

``attach_readiness_route`` lives in ``forze_fastapi``, which means the processes most
likely to wedge silently — the outbox relay, inbox and commit-stream consumers, the
socket.io gateway, durable runners — expose nothing a Kubernetes probe can hit. Their
health is today only *inferable*, from gauges like ``forze.realtime.backplane
.seconds_since_ok``, and a gauge is an alerting signal, not a restart signal.

This is the smallest thing that closes that gap: one ``asyncio`` server, a hand-rolled
HTTP/1.0 responder, and the readiness answer the FastAPI route gives — both call
:func:`evaluate_readiness`, so the same scope and the same dependency checks answer the same
way on either. **Stdlib only, deliberately** — a utility container must not grow FastAPI or
aiohttp to answer two endpoints, and there is nothing here worth the dependency: no
routing, no request bodies, no keep-alive.
"""

from __future__ import annotations

import asyncio
import json
import warnings
from collections.abc import Mapping
from contextlib import suppress
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final, cast, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.deps import DepKey
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.tenancy import require_tenant_global
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKey

from ..context.lifecycle import scope_readiness
from .loop import DEFAULT_STOP_GRACE_SECONDS

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext
    from forze.application.execution.runtime import ExecutionRuntime

# ----------------------- #

__all__ = [
    "PROBE_LISTENER_STEP_ID",
    "evaluate_readiness",
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


async def evaluate_readiness(
    ctx: ExecutionContext | None,
    checks: Mapping[str, DepKey[Any]] | None = None,
    *,
    check_timeout: timedelta = timedelta(seconds=2),
) -> tuple[int, dict[str, Any]]:
    """Answer a readiness probe for *ctx*: the HTTP status and the JSON body.

    ``503`` with ``{"status": "unavailable"}`` without a scope (*ctx* is ``None``) or before
    its lifecycle startup finished, and ``{"status": "draining"}`` once a drain was announced
    or the drain gate closed — answered before any check runs. Otherwise each check's client
    is resolved from ``ctx.deps`` and asked ``health()``, concurrently, each under
    *check_timeout*; any failure makes the answer ``503 degraded``::

        {"status": "degraded",
         "checks": {"postgres": {"ok": true,  "detail": "ok"},
                    "redis":    {"ok": false, "detail": "timed out after 2.0s"}}}

    No *checks* answers ``200 {"status": "ready"}``, with no ``checks`` key at all.

    :param ctx: The scope to answer for, or ``None`` when no scope is active.
    :param checks: Check name (the key in ``checks``) to the deps key its client is
        registered under, plain: a key registered only per route names no single client and
        is reported as such. To check a client registered per route, register it under a
        plain key of its own as well.
        A check runs with no tenant bound, so a client routed per tenant is reported as
        ``routed per tenant``.
    :param check_timeout: Budget for **each** check, not for the sweep.
    :returns: ``(status code, body)``.
    """

    if ctx is None:
        return 503, {"status": "unavailable"}

    status = scope_readiness(ctx)

    if status != "ready":
        return 503, {"status": status}

    if not checks:
        return 200, {"status": "ready"}

    results = await _check_dependencies(ctx, checks, check_timeout)

    # The checks took time: a drain announced meanwhile wins over their answer.
    status = scope_readiness(ctx)

    if status != "ready":
        return 503, {"status": status}

    healthy = all(result["ok"] for result in results.values())

    return (200 if healthy else 503), {
        "status": "ready" if healthy else "degraded",
        "checks": results,
    }


# ....................... #


async def _check_dependencies(
    ctx: ExecutionContext,
    checks: Mapping[str, DepKey[Any]],
    timeout: timedelta,
) -> dict[str, dict[str, Any]]:
    """Ask every named client ``health()`` concurrently, one timeout each."""

    seconds = timeout.total_seconds()
    deps = ctx.deps

    async def check(name: str, key: DepKey[Any]) -> tuple[str, dict[str, Any]]:
        # Every failure below becomes a *reported* one. A check that raises turns readiness
        # into a 500 — indistinguishable from an unrelated bug — and takes the breakdown
        # with it, in exactly the situation the breakdown exists for.
        try:
            if not deps.exists(key):
                # A check resolves its key plain. One registered only per route is not
                # missing, it names no single client: the report says which it is.
                routed = any(
                    frame.key_name == key.name and frame.route is not None
                    for frame in deps.registered_frames()
                )

                return name, {
                    "ok": False,
                    "detail": "registered per route only" if routed else "not registered",
                }

            # Per check, deliberately, not one deadline around the sweep. The two look
            # equivalent — the checks run concurrently, so the wall clock is the same — but
            # a sweep-wide timeout cancels *every* check when one dependency's driver
            # retries past the deadline, and answers 503 with `checks: {}`. Per check, a
            # hanging dependency is one failed check with a name on it.
            client = deps.provide(key)

            try:
                require_tenant_global(client, what=f"Readiness check {name!r}")

            except CoreException:
                # The one refusal it raises. A probe runs with no tenant bound, so a client
                # routed per tenant names no client to probe either, just as a key
                # registered only per route does.
                return name, {"ok": False, "detail": "routed per tenant"}

            budget = asyncio.timeout(seconds)

            try:
                async with budget:
                    detail, ok = await client.health()

            except TimeoutError:
                # Only *our* budget is reported as ours. A driver with its own deadline
                # raises `TimeoutError` too, and calling that "timed out after 2.0s" names
                # a deadline that never elapsed — it goes to the handler below instead,
                # which reports it by type like any other client failure.
                if not budget.expired():
                    raise

                return name, {"ok": False, "detail": f"timed out after {seconds}s"}

            return name, {"ok": bool(ok), "detail": str(detail)}

        except Exception as error:
            logger.warning("Readiness check %s failed", name, exc_info=True)

            # The type, not the message: a driver's connection error routinely carries the
            # DSN, and this body is served to anything that can reach the probe.
            return name, {"ok": False, "detail": type(error).__name__}

    return dict(await asyncio.gather(*(check(name, key) for name, key in checks.items())))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _ProbeListenerStartup(LifecycleHook):
    """Serve the two probe paths for as long as the process should answer them."""

    runtime: ExecutionRuntime | None
    """The deprecated watched runtime; ``None`` answers from the hook's own ``ctx``."""

    host: str
    port: int
    path_ready: str
    path_live: str
    checks: Mapping[str, DepKey[Any]] | None
    check_timeout: timedelta

    ctx: ExecutionContext | None = attrs.field(default=None, init=False, repr=False)
    """The scope this listener was started in, read on every readiness request."""

    server: asyncio.Server | None = attrs.field(default=None, init=False, repr=False)

    stopping: bool = attrs.field(default=False, init=False, repr=False)
    """Latched by :meth:`stop` before it hangs up, so connections accepted in the same
    breath refuse themselves instead of being missed.

    ``asyncio`` accepts a socket and *schedules* its handler, so there is a window where a
    connection exists but its handler has not run its first line yet — long enough for a
    stop to snapshot the connection set without it. That socket would then park in
    ``readline`` for the full read timeout while ``wait_closed()`` waited on it."""

    connections: set[asyncio.StreamWriter] = attrs.field(factory=set, init=False, repr=False)
    """Live client transports, so shutdown can hang up on them.

    ``Server.wait_closed()`` waits for every handler to finish, and a handler sitting in
    ``readline`` waits out the full read timeout. Without this, one idle socket — a port
    scanner, a half-open load-balancer check — would hold the graceful stop open past the
    drain budget and log a "did not close" warning about a listener that had no work left.
    """

    handlers: set[asyncio.Task[Any]] = attrs.field(factory=set, init=False, repr=False)
    """Request handlers still running, so shutdown can cancel them. Closing a connection
    does not stop its handler: one waiting on a slow dependency check would otherwise run on
    after the stop, against clients teardown is about to close."""

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
        self.stopping = True
        server.close()

        # Closing the listening socket only stops *new* connections. Existing ones are
        # hung up on here: a probe response is a single write with no state behind it, so
        # there is nothing in flight worth waiting for. Anything accepted but not yet
        # started is covered by the flag above rather than by this snapshot.
        for writer in tuple(self.connections):
            writer.close()

        handlers = tuple(self.handlers)

        for task in handlers:
            task.cancel()

        clock = asyncio.get_running_loop()
        budget = max(0.0, deadline - clock.time())

        try:
            await asyncio.wait_for(asyncio.shield(server.wait_closed()), timeout=budget)

            if handlers:
                _done, pending = await asyncio.wait(
                    handlers, timeout=max(0.0, deadline - clock.time())
                )

                if pending:
                    raise TimeoutError

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

        self.ctx = ctx

        # Cleared here, not in ``stop``: a listener that went down stays refusing until it
        # is deliberately brought back up.
        self.stopping = False
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
        if self.stopping:
            # Accepted just as the listener was going down. Hang up rather than read: this
            # handler is one of the tasks ``wait_closed()`` is waiting on.
            writer.close()

            return

        # ``asyncio.start_server`` runs every handler as a task of its own.
        handler = cast("asyncio.Task[Any]", asyncio.current_task())
        self.handlers.add(handler)
        self.connections.add(writer)

        try:
            method, (status, payload) = await self._answer(reader)
            # HEAD carries the headers of the equivalent GET and none of its body.
            writer.write(_response(status, payload, with_body=method != "HEAD"))
            await writer.drain()

        except (TimeoutError, ConnectionError, asyncio.IncompleteReadError):
            # A probe client that hangs up mid-request is routine (kubelet timeouts,
            # port scans); it must never surface as an error in a worker's logs.
            pass

        except Exception:
            logger.warning("Probe listener request failed", exc_info=True)

        finally:
            self.connections.discard(writer)
            self.handlers.discard(handler)
            writer.close()

            with suppress(ConnectionError, OSError):
                await writer.wait_closed()

    # ....................... #

    async def _answer(
        self,
        reader: asyncio.StreamReader,
    ) -> tuple[str, tuple[int, dict[str, Any]]]:
        """The request's method, and the status/body to answer it with."""

        try:
            line = await asyncio.wait_for(reader.readline(), timeout=_READ_TIMEOUT)

        except ValueError:
            # The stream's own buffer limit (64 KiB) overflowed before our cap could
            # apply. Still just a malformed request — answering 400 keeps a port scanner
            # from writing a stack trace into a worker's logs on every connection.
            return "GET", (400, {"status": "bad_request"})

        if not line or len(line) > _REQUEST_LINE_LIMIT:
            return "GET", (400, {"status": "bad_request"})

        parts = line.decode("latin-1", errors="replace").split()

        if len(parts) < 2 or parts[0] not in ("GET", "HEAD"):
            return "GET", (400, {"status": "bad_request"})

        method = parts[0]
        path = parts[1].split("?", 1)[0]

        if path == self.path_live:
            # Same contract as the FastAPI route: reaching here *is* the check, and the
            # dependency checks are no part of it.
            return method, (200, {"status": "alive"})

        if path != self.path_ready:
            return method, (404, {"status": "not_found"})

        return method, await evaluate_readiness(
            self._watched_context(),
            self.checks,
            check_timeout=self.check_timeout,
        )

    # ....................... #

    def _watched_context(self) -> ExecutionContext | None:
        if self.runtime is None:
            return self.ctx

        try:
            return self.runtime.get_context()

        except CoreException:
            return None


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


def _response(status: int, payload: Mapping[str, Any], *, with_body: bool = True) -> bytes:
    content = json.dumps(payload, separators=(",", ":")).encode()
    head = (
        f"HTTP/1.0 {status} {_STATUS_TEXT[status]}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(content)}\r\n"
        f"Cache-Control: no-store\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode()

    return head + content if with_body else head


# ----------------------- #


def probe_listener_step(
    runtime: ExecutionRuntime | None = None,
    *,
    host: str = "0.0.0.0",  # nosec B104 - a kubelet probe reaches the pod from outside it
    port: int,
    path_ready: str = "/readyz",
    path_live: str = "/livez",
    checks: Mapping[str, DepKey[Any]] | None = None,
    check_timeout: timedelta = timedelta(seconds=2),
    step_id: StrKey = PROBE_LISTENER_STEP_ID,
) -> LifecycleStep:
    """Build a lifecycle step serving ``/livez`` and ``/readyz`` for a non-HTTP process.

    Wire it into any runner's lifecycle — outbox relay, queue or stream consumer, socket.io
    gateway, durable runner — and point Kubernetes at it: liveness on *path_live*,
    readiness on *path_ready*, startup on *path_ready* with a generous failure threshold.

    Readiness answers for the scope whose lifecycle started the step, through
    :func:`evaluate_readiness` — the evaluator the FastAPI route uses: ``ready`` once that
    scope's lifecycle startup finished and while every check passes, ``draining`` from the
    moment a drain is announced, ``unavailable`` while startup is still running, ``degraded``
    while a check fails. Liveness answers ``200`` unconditionally and runs no check, because
    a process whose event loop still schedules this handler is by definition alive.

    Probes remain the *restart* signal, not the alerting one: keep alerting on the gauges
    (backplane freshness, mailbox overflow, job staleness), which detect a loop that is
    running but making no progress — something no probe can see.

    :param runtime: Deprecated. The step reads its scope from the ``ctx`` its startup hook
        receives, which is what lets it sit in its own runtime's lifecycle plan. When given,
        readiness answers from this runtime's scope instead, as it used to.
    :param host: Bind address. Defaults to all interfaces, because the probe request comes
        from the node, not from inside the container.
    :param port: Bind port; pick one per process (8079 by convention for workers).
    :param checks: Check name (the key in the response's ``checks``) to the deps key of a
        client whose ``health()`` readiness depends on. Keep the port internal: a failing
        driver's detail routinely names its host and user.
    :param check_timeout: Budget for **each** check, not for the sweep.

    :raises CoreException: ``configuration`` — the port is out of range, the two probe
        paths are the same, or *checks* is set with a *check_timeout* that is not positive.
    """

    if runtime is not None:
        warnings.warn(
            "probe_listener_step(runtime=...) is deprecated: the step reads its scope from "
            "its hook's ctx; drop the argument and place the step in the runtime's own plan",
            DeprecationWarning,
            stacklevel=2,
        )

    if checks and check_timeout <= timedelta(0):
        # Every check would report a timeout it never waited for, so the endpoint answers a
        # permanent `degraded` that looks exactly like every dependency being down.
        raise exc.configuration(
            f"Probe listener needs a positive check_timeout, got {check_timeout}"
        )

    if not 1 <= port <= 65_535:
        raise exc.configuration(f"Probe listener port must be in 1..65535, got {port}")

    for name, path in (("path_ready", path_ready), ("path_live", path_live)):
        # Requests are matched on the path with its query stripped, so a configured path
        # carrying one can never be reached — and the *other* probe silently answers in
        # its place. Configure `/readyz?check=1` beside `/livez` and every readiness
        # request resolves to liveness: a draining pod answers 200 and keeps taking
        # traffic for the whole drain window, which is the one failure splitting these
        # two endpoints exists to prevent.
        if not path.startswith("/") or "?" in path or path.split() != [path]:
            raise exc.configuration(
                f"Probe listener {name} must be a plain path starting with '/' and "
                f"carrying no query string or whitespace, got {path!r}"
            )

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
        checks=checks,
        check_timeout=check_timeout,
    )

    return LifecycleStep(
        id=step_id,
        startup=startup,
        shutdown=_ProbeListenerShutdown(startup=startup),
        requires_long_running=True,
    )
