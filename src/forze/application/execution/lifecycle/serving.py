"""Hold a runtime's scope open until the process is told to stop, and drain it first.

A process that serves no HTTP — an outbox relay, a consumer, a worker — still has to turn
``SIGTERM`` into a drain: as PID 1 in a container it otherwise ignores the signal, and is
killed at the end of the grace period. :func:`serve_runtime` does that, and
:class:`SignalDrain` is the signal handling it shares with ``forze_fastapi.serve_uvicorn``.
"""

import asyncio
import signal
import threading
from collections.abc import Generator
from contextlib import contextmanager, suppress
from contextvars import Context, copy_context
from datetime import timedelta
from types import FrameType
from typing import TYPE_CHECKING, Any, ClassVar, Final

from forze.application._logger import logger
from forze.base.exceptions import CoreException
from forze.base.logging import Logger

from .run import DRAIN_PHASE_CANCEL_GRACE_SECONDS

if TYPE_CHECKING:
    from ..runtime import ExecutionRuntime

# ----------------------- #

_HANDLED_SIGNALS: Final[tuple[signal.Signals, ...]] = (signal.SIGINT, signal.SIGTERM)

# ....................... #


class SignalDrain:
    """The first SIGTERM or SIGINT, or *stop*, drains the runtime; a SIGINT during the drain
    cancels it.

    Build it on the loop, in the task whose context — no request's — the drain runs in.
    :meth:`_drain_ended` runs once the drain ends, however it ends, and :meth:`_forced` once
    a SIGINT cut it short; a subclass extends them to stop whatever it serves.
    """

    log: ClassVar[Logger] = logger

    def __init__(
        self,
        runtime: "ExecutionRuntime",
        *,
        drain_budget: timedelta,
        announce_delay: timedelta,
        stop: asyncio.Event | None = None,
    ) -> None:
        self._runtime = runtime
        self._drain_budget = drain_budget
        self._announce_delay = announce_delay
        self._stop = stop
        # The loop the signal handler hands over to, and the context its callback runs in.
        self._loop = asyncio.get_running_loop()
        self._context: Context = copy_context()
        self._drain_task: asyncio.Task[None] | None = None

        self.drained = asyncio.Event()
        """Set once the drain ends, however it ends."""

    # ....................... #

    @contextmanager
    def capture_signals(self) -> Generator[None]:
        """Handle the signals, and watch *stop*, for the duration of the block."""

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
                # ``None``: installed from C, which Python cannot put back. The default, not
                # ours left pointing at a closed loop, so a later SIGTERM still terminates.
                signal.signal(sig, signal.SIG_DFL if handler is None else handler)

            if watch is not None:
                watch.cancel()

            self.cancel_drain()

        # Unlike uvicorn's own handler, the signal is not raised again on the way out: the
        # process decides how it exits.

    # ....................... #

    def cancel_drain(self) -> None:
        """Cancel the drain, if one is running."""

        if self._drain_task is not None and not self._drain_task.done():
            self._drain_task.cancel()

    # ....................... #

    async def cancel_drain_and_wait(self) -> None:
        """Cancel the drain, if one is running, let it unwind — a second at most — and give
        the drain phases up, so a teardown that follows does not run them."""

        task = self._drain_task

        # Already cancelled counts too: its done callback may still be queued behind this.
        if task is not None and (task.cancel() or task.cancelled()):
            # Not left to the done callback: a teardown that starts before it runs would take
            # back the claim the cancelled drain returns, and run the phases at length.
            await asyncio.wait({task}, timeout=DRAIN_PHASE_CANCEL_GRACE_SECONDS)
            self._forgo()

    # ....................... #

    def _on_signal(self, sig: int, _frame: FrameType | None) -> None:
        # Runs on the main thread between two bytecodes, possibly while the loop sleeps in
        # its selector: hand over to the loop, which ``call_soon_threadsafe`` wakes. In the
        # captured context — by default the callback would copy the context of whatever
        # the signal interrupted, a request handler's bindings included.
        self._loop.call_soon_threadsafe(self._on_signal_in_loop, sig, context=self._context.copy())

    # ....................... #

    async def _watch(self, stop: asyncio.Event) -> None:
        await stop.wait()

        if self._drain_task is None:
            self.log.info("Stop requested: draining before shutdown")
            self._start_drain()

    # ....................... #

    def _on_signal_in_loop(self, sig: int) -> None:
        name = signal.Signals(sig).name

        if self._drain_task is None:
            self.log.info("%s received: draining before shutdown", name)
            self._start_drain()

            return

        if self.drained.is_set():
            self.log.warning("%s received after the drain: teardown in progress", name)

            # A SIGINT still forces the exit, for a server still waiting on connections.
            if sig == signal.SIGINT:
                self._forced()

            return

        if sig != signal.SIGINT:
            # Only a SIGINT forces the exit, as with uvicorn. A SIGTERM sent twice — to the
            # process group and again by a wrapper forwarding it — must not cut the drain.
            self.log.warning("%s received again: still draining; a SIGINT forces the exit", name)

            return

        self.log.warning("%s received during the drain: exiting without waiting for it", name)
        self._drain_task.cancel()
        self._forced()

    # ....................... #

    def _start_drain(self) -> None:
        self._drain_task = asyncio.ensure_future(self._drain())
        # A done callback, not a ``finally`` in the coroutine: a task cancelled before its
        # first step — a second signal handled in the same loop turn — never runs its body.
        self._drain_task.add_done_callback(self._drain_done)

    # ....................... #

    async def _drain(self) -> None:
        try:
            await self._runtime.drain(self._drain_budget, announce_delay=self._announce_delay)

        except Exception:
            self.log.exception("Drain failed; shutting down anyway")

    # ....................... #

    def _drain_done(self, task: "asyncio.Task[None]") -> None:
        if task.cancelled():
            # A forced exit. A drain cut short before its phases started gives its claim
            # back, and the scope's teardown would then run them at length: give them up.
            self._forgo()

        self._drain_ended()

    # ....................... #

    def _forgo(self) -> None:
        with suppress(CoreException):
            self._runtime.get_context().drain_phases.forgo()

    # ....................... #

    def _drain_ended(self) -> None:
        self.drained.set()

    # ....................... #

    def _forced(self) -> None:
        """Nothing beyond the cancelled drain by default."""


# ....................... #


async def serve_runtime(
    runtime: "ExecutionRuntime",
    *,
    drain_budget: timedelta,
    announce_delay: timedelta = timedelta(0),
    stop: asyncio.Event | None = None,
) -> None:
    """Hold *runtime*'s scope open until the process is told to stop, draining it first.

    The counterpart of ``forze_fastapi.serve_uvicorn`` for a process that serves no HTTP —
    one whose work is its lifecycle's background steps. On the first SIGTERM or SIGINT,
    ``runtime.drain(drain_budget, announce_delay=announce_delay)`` withdraws readiness and
    runs the drain phases; then the scope exits, closing its gate and tearing down, and this
    returns. A drain that fails still tears down. A SIGINT during the drain cancels it and
    gives the drain phases up, and teardown follows at once; a repeated SIGTERM is logged
    and ignored, so a signal delivered twice cannot cut a deploy's drain short. The handlers
    stay installed through teardown. Cancelling the call cancels a drain in flight, as that
    SIGINT does; the scope still tears down, and the cancellation is raised.

    Setting *stop* does what the first signal does, from any thread of the process — set it
    on the runtime's loop (``loop.call_soon_threadsafe(stop.set)``). Off the main thread it
    is the only way in: no signal handler can be installed there, and none is. The previous
    handlers are restored on return, and the signal is not raised again.

    A signal during lifecycle startup finds nothing to drain yet: the scope exits once
    startup finishes, and its shutdown runs the drain phases with the runtime's
    ``drain_budget`` instead.

    :param runtime: The runtime whose scope this holds; nothing else may hold it.
    :param drain_budget: Shared by the drain phases.
    :param announce_delay: How long readiness fails before the first phase starts.
    :param stop: Set it to drain and stop, as the first signal would.
    """

    signals = SignalDrain(
        runtime,
        drain_budget=drain_budget,
        announce_delay=announce_delay,
        stop=stop,
    )

    with signals.capture_signals():
        async with runtime.scope():
            try:
                await signals.drained.wait()

            finally:
                # Cancelled from outside mid-drain: the scope's shutdown would wait it out, or
                # run the phases itself.
                await signals.cancel_drain_and_wait()
