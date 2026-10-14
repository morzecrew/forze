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
import threading
from collections.abc import Generator
from contextlib import contextmanager
from contextvars import Context, copy_context
from datetime import timedelta
from types import FrameType
from typing import Any, Final

import uvicorn

from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import exc
from forze.base.logging import Logger

# ----------------------- #

_logger = Logger("fastapi.server")

_HANDLED_SIGNALS: Final[tuple[signal.Signals, ...]] = (signal.SIGINT, signal.SIGTERM)


# ....................... #


class _DrainingServer(uvicorn.Server):
    """A uvicorn server whose first SIGTERM/SIGINT drains the runtime before it exits."""

    def __init__(
        self,
        config: uvicorn.Config,
        runtime: ExecutionRuntime,
        *,
        drain_budget: timedelta,
        announce_delay: timedelta,
        stop: asyncio.Event | None = None,
    ) -> None:
        super().__init__(config)

        self._runtime = runtime
        self._drain_budget = drain_budget
        self._announce_delay = announce_delay
        self._stop = stop
        self._loop: asyncio.AbstractEventLoop | None = None
        self._context: Context = copy_context()
        self._drain_task: asyncio.Task[None] | None = None

    # ....................... #

    @contextmanager
    def capture_signals(self) -> Generator[None]:
        self._loop = asyncio.get_running_loop()
        self._context = copy_context()
        watch = None if self._stop is None else self._loop.create_task(self._watch(self._stop))
        previous: dict[int, Any] = {}

        # Only the main thread can install signal handlers. Off it none are installed, as
        # uvicorn does, and the stop event is the way in. ``signal.signal`` rather than the
        # loop's handlers, as uvicorn does, so the previous handlers can be put back.
        if threading.current_thread() is threading.main_thread():
            previous = {sig: signal.signal(sig, self._on_signal) for sig in _HANDLED_SIGNALS}

        try:
            yield

        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)

            if watch is not None:
                watch.cancel()

            if self._drain_task is not None and not self._drain_task.done():
                self._drain_task.cancel()

        # Unlike uvicorn's own handler, the signal is not raised again once the server has
        # returned: the process that embeds the server decides how it exits.

    # ....................... #

    def _on_signal(self, sig: int, _frame: FrameType | None) -> None:
        # Runs on the main thread between two bytecodes, possibly while the loop sleeps in
        # its selector: hand over to the loop, which ``call_soon_threadsafe`` wakes. In the
        # server's own context — by default the callback would copy the context of whatever
        # the signal interrupted, a request handler's bindings included.
        if self._loop is not None:
            self._loop.call_soon_threadsafe(
                self._on_signal_in_loop, sig, context=self._context.copy()
            )

    # ....................... #

    async def _watch(self, stop: asyncio.Event) -> None:
        await stop.wait()

        if self._drain_task is None:
            _logger.info("Stop requested: draining before the server stops")
            self._drain_task = asyncio.ensure_future(self._drain())

    # ....................... #

    def _on_signal_in_loop(self, sig: int) -> None:
        name = signal.Signals(sig).name

        if self._drain_task is None:
            _logger.info("%s received: draining before the server stops", name)
            self._drain_task = asyncio.ensure_future(self._drain())

            return

        if sig != signal.SIGINT:
            # Only a SIGINT forces the exit, as with uvicorn. A SIGTERM sent twice — to the
            # process group and again by a wrapper forwarding it — must not cut the drain.
            _logger.warning("%s received again: still draining; a SIGINT forces the exit", name)

            return

        # ``force_exit`` alone would not stop a drain in progress, and ``should_exit`` alone
        # would still wait for open connections.
        _logger.warning("%s received during the drain: exiting without waiting for it", name)
        self._drain_task.cancel()
        self.should_exit = True
        self.force_exit = True

    # ....................... #

    async def _drain(self) -> None:
        try:
            await self._runtime.drain(self._drain_budget, announce_delay=self._announce_delay)

        except Exception:
            _logger.exception("Drain failed; stopping the server anyway")

        finally:
            self.should_exit = True


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

    An orderly exit takes up to ``announce_delay + drain_budget + graceful timeout +
    runtime.drain_timeout`` plus lifecycle shutdown, and that has to fit in the
    orchestrator's grace period. The figures are logged when the server starts.

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

    if config.timeout_graceful_shutdown is None:
        # Without one uvicorn waits for open streams forever, so one SSE client turns every
        # restart into a kill at the grace period.
        config.timeout_graceful_shutdown = math.ceil(graceful_timeout.total_seconds())

    _logger.info(
        "Exit budget: announce %.1fs + drain %.1fs + graceful %ss + drain_timeout %.1fs "
        "+ lifecycle shutdown (up to %.1fs per step)",
        announce_delay.total_seconds(),
        drain_budget.total_seconds(),
        config.timeout_graceful_shutdown,
        runtime.drain_timeout.total_seconds(),
        runtime.shutdown_step_timeout.total_seconds(),
    )

    server = _DrainingServer(
        config,
        runtime,
        drain_budget=drain_budget,
        announce_delay=announce_delay,
        stop=stop,
    )

    await server.serve()
