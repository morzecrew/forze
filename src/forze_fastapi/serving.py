"""Serve an app under an embedded uvicorn whose shutdown signal drains the runtime first.

``uvicorn.Server.serve()`` replaces the process's SIGTERM and SIGINT handlers for as long as
it runs, with one that tells the server to exit at once. An app that embeds uvicorn and
installs its own handler loses it, so every restart skips the drain: the server stops
accepting, closes every WebSocket with 1012, waits — without limit, unless a graceful timeout
is set — for open HTTP streams, and only then does the lifespan shutdown run. This module
gives uvicorn a signal handler that drains the runtime *first*, while the server still
serves, and only then lets it exit.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import asyncio
import math
import signal
from collections.abc import Generator
from contextlib import contextmanager
from datetime import timedelta

import uvicorn

from forze.application.execution import ExecutionRuntime
from forze.application.execution.lifecycle.run import DRAIN_PHASE_CANCEL_GRACE_SECONDS
from forze.application.execution.lifecycle.serving import SignalDrain
from forze.base.exceptions import exc
from forze.base.logging import Logger

# ----------------------- #

_logger = Logger("fastapi.server")


# ....................... #


class _DrainingServer(SignalDrain, uvicorn.Server):
    """A uvicorn server whose first SIGTERM/SIGINT drains the runtime before it exits."""

    log = _logger

    def __init__(
        self,
        config: uvicorn.Config,
        runtime: ExecutionRuntime,
        *,
        drain_budget: timedelta,
        announce_delay: timedelta,
        stop: asyncio.Event | None = None,
    ) -> None:
        uvicorn.Server.__init__(self, config)
        # Built by ``serve_uvicorn`` on the loop and in the task that serves: the loop the
        # signal handler hands over to, and the context — the server's own, no request's —
        # its callback runs in.
        SignalDrain.__init__(
            self,
            runtime,
            drain_budget=drain_budget,
            announce_delay=announce_delay,
            stop=stop,
        )

    # ....................... #

    def _drain_ended(self) -> None:
        super()._drain_ended()
        self.should_exit = True

    # ....................... #

    def _forced(self) -> None:
        # ``force_exit`` alone would not stop a drain in progress, and ``should_exit`` alone
        # would still wait for open connections.
        self.should_exit = True
        self.force_exit = True


# ....................... #


async def serve_uvicorn(
    config: uvicorn.Config,
    runtime: ExecutionRuntime,
    *,
    drain_budget: timedelta,
    announce_delay: timedelta = timedelta(0),
    graceful_timeout: timedelta = timedelta(seconds=10),
    stop: asyncio.Event | None = None,
) -> None:
    """Serve *config*'s app with uvicorn, draining *runtime* before the server stops.

    On the first SIGTERM or SIGINT the server keeps serving while
    ``runtime.drain(drain_budget, announce_delay=announce_delay)`` withdraws readiness and
    runs the drain phases; then it exits the way uvicorn does — stops accepting, finishes
    in-flight requests within the graceful timeout, runs the lifespan shutdown, where the
    runtime closes its gate and tears down. A drain that fails still lets the server exit. A
    SIGINT after that cancels the drain and exits without waiting for open connections — a
    repeated SIGTERM is logged and ignored, so a signal delivered twice cannot cut a deploy's
    drain short. Uvicorn then skips the lifespan shutdown, so the runtime tears down only when the event loop
    closes and cancels the lifespan task (at the end of ``asyncio.run``).

    Setting *stop* does what the first signal does, from any thread of the process —
    set it on the server's loop (``loop.call_soon_threadsafe(stop.set)``). Off the main
    thread it is the only way in: no signal handler can be installed there, and none is.
    The previous handlers are restored on return, and the signal is not raised again.

    A signal during the lifespan startup finds nothing to drain yet: the server exits once
    startup finishes, and the drain phases run in the lifespan shutdown instead — after the
    server stopped accepting, where a later signal cannot cut them short.

    The app must hold *runtime*'s scope open — ``FastAPI(lifespan=runtime_lifespan(runtime))``.
    For a runtime the lifespan builds, or several worker processes, give ``runtime_lifespan``
    a *drain_budget* instead (:func:`drain_ahead_of_uvicorn`).

    An orderly exit takes up to ``announce_delay + drain_budget + 1 s`` (a phase that
    ignores its cancellation) ``+ graceful timeout + runtime.drain_timeout +
    runtime.shutdown_step_timeout × (3 + lifecycle steps)`` — the three being cancelling the
    operations ``drain_timeout`` left running, stopping the background loops, and closing
    detached background work. That worst case has to fit in the orchestrator's grace
    period; it is logged, figure by figure, when the server starts.

    :param config: The uvicorn configuration. ``timeout_graceful_shutdown`` is set to
        *graceful_timeout*, rounded up to whole seconds, when unset; a value already set is
        kept.
    :param runtime: The runtime whose scope the app's lifespan holds.
    :param drain_budget: Shared by the drain phases.
    :param announce_delay: How long readiness fails before the first phase starts.
    :param graceful_timeout: How long the server waits for open connections — an SSE
        stream, a long poll — before cancelling them.
    :param stop: Set it to drain and stop, as the first signal would.

    :raises CoreException: ``configuration`` — the installed uvicorn has no
        ``Server.capture_signals``.
    """

    if not callable(getattr(uvicorn.Server, "capture_signals", None)):
        raise exc.configuration(
            "serve_uvicorn needs uvicorn.Server.capture_signals, which the installed uvicorn "
            "does not have; without it a signal would stop the server without draining"
        )

    _log_exit_budget(
        config,
        runtime,
        drain_budget=drain_budget,
        announce_delay=announce_delay,
        graceful_timeout=graceful_timeout,
    )

    server = _DrainingServer(
        config,
        runtime,
        drain_budget=drain_budget,
        announce_delay=announce_delay,
        stop=stop,
    )

    await server.serve()


# ....................... #


def _log_exit_budget(
    config: uvicorn.Config,
    runtime: ExecutionRuntime,
    *,
    drain_budget: timedelta,
    announce_delay: timedelta,
    graceful_timeout: timedelta,
) -> None:
    if config.timeout_graceful_shutdown is None:
        # Without one uvicorn waits for open streams forever, so one SSE client turns every
        # restart into a kill at the grace period.
        config.timeout_graceful_shutdown = math.ceil(graceful_timeout.total_seconds())

    steps = len(runtime.lifecycle.graph.steps)
    step_timeout = runtime.shutdown_step_timeout.total_seconds()
    figures = (
        announce_delay.total_seconds(),
        drain_budget.total_seconds(),
        DRAIN_PHASE_CANCEL_GRACE_SECONDS,
        float(config.timeout_graceful_shutdown),
        runtime.drain_timeout.total_seconds(),
        step_timeout * (3 + steps),
    )

    _logger.info(
        "Worst-case exit %.1fs: announce %.1fs + drain %.1fs + abandoned phase %.1fs "
        "+ graceful %.1fs + drain_timeout %.1fs + shutdown %.1fs (%.1fs x (3 + %d step(s))); "
        "fit it inside the orchestrator's grace period",
        sum(figures),
        *figures,
        step_timeout,
        steps,
    )


# ....................... #


class _ServerDrain(SignalDrain):
    """Drains the runtime ahead of the uvicorn server whose signal handler it displaced."""

    log = _logger

    def __init__(
        self,
        server: uvicorn.Server,
        runtime: ExecutionRuntime,
        *,
        drain_budget: timedelta,
        announce_delay: timedelta,
    ) -> None:
        super().__init__(runtime, drain_budget=drain_budget, announce_delay=announce_delay)
        self._server = server

    # ....................... #

    def _drain_ended(self) -> None:
        super()._drain_ended()
        self._server.should_exit = True

    # ....................... #

    def _forced(self) -> None:
        self._server.should_exit = True
        self._server.force_exit = True


# ....................... #


@contextmanager
def drain_ahead_of_uvicorn(
    runtime: ExecutionRuntime,
    *,
    drain_budget: timedelta,
    announce_delay: timedelta,
) -> Generator[None]:
    """Drain *runtime* on the first SIGTERM or SIGINT before the uvicorn server serving this
    process stops, for the duration of the block.

    Enter it on the server's loop, from the app's lifespan (``runtime_lifespan`` does, given
    a *drain_budget*): it takes over the signal handler ``uvicorn.Server`` installed — the
    one each worker of ``uvicorn --workers N`` and of gunicorn's uvicorn worker runs too —
    and on the first signal runs the drain while the server still serves, then tells the
    server to exit. The signals behave as under :func:`serve_uvicorn`, whose server already
    drains: under it this does nothing. Under any other handler — another server, or a
    server off the main thread, which installs none — it logs a warning and does nothing.
    Unless the server sets ``timeout_graceful_shutdown``, it is set to 10 seconds.

    :param runtime: The runtime whose scope the block holds.
    :param drain_budget: Shared by the drain phases.
    :param announce_delay: How long readiness fails before the first phase starts.
    """

    handler = signal.getsignal(signal.SIGTERM)
    server = getattr(handler, "__self__", None)

    if isinstance(server, SignalDrain):
        # serve_uvicorn's server, which drains the runtime itself.
        yield
        return

    if not isinstance(server, uvicorn.Server):
        _logger.warning(
            "SIGTERM is not handled by a uvicorn server on this process's main thread: "
            "the runtime drains only after the server stops accepting"
        )

        yield
        return

    _log_exit_budget(
        server.config,
        runtime,
        drain_budget=drain_budget,
        announce_delay=announce_delay,
        graceful_timeout=timedelta(seconds=10),
    )

    drain = _ServerDrain(
        server,
        runtime,
        drain_budget=drain_budget,
        announce_delay=announce_delay,
    )

    with drain.capture_signals():
        yield
