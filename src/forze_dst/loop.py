"""A native deterministic asyncio event loop for simulation — virtual time, no real I/O.

CPython's :class:`asyncio.BaseEventLoop` already implements *all* scheduling
(``call_soon`` / ``call_later`` / ``Task`` / ``sleep`` / ``wait_for``) in terms of
two hooks: ``loop.time()`` and ``selector.select(timeout)``. So a deterministic
loop is small — override those two:

* :meth:`SimulationEventLoop.time` returns a virtual clock that only ever advances
  when the loop explicitly waits;
* the :class:`_NullSelector` does no real I/O and, instead of blocking for
  ``timeout`` seconds, *advances the virtual clock* by ``timeout`` and returns no
  events. ``await asyncio.sleep(60)`` then completes instantly while the simulated
  clock jumps 60 seconds — same seed, same schedule, every run.

Refusing real I/O (sockets, pipes, threads) is deliberate: it doubles as a leak
guard, turning any stray real-world call during a simulation into a loud error
rather than a non-deterministic, wall-clock-bound hang.
"""

from __future__ import annotations

import asyncio
import random
import selectors
from typing import Any, Mapping, cast

# ----------------------- #


class RealIOForbidden(RuntimeError):
    """A simulation tried to touch the real world (socket / pipe / thread / signal).

    Simulations run on an isolated virtual-time loop with no real I/O. This is the
    leak guard: it points at the exact call that escaped the in-memory substrate."""


# ....................... #


class SimulationDeadlock(RuntimeError):
    """The simulation has no ready work and no pending timer — it would block forever.

    Surfaced instead of hanging: a genuine deadlock in the scenario (e.g. awaiting an
    event that nothing will ever fire), since virtual time has nothing to advance to."""


# ....................... #


def _forbidden(what: str) -> RealIOForbidden:
    return RealIOForbidden(
        f"{what} is not available inside a simulation (no real I/O / threads); "
        "use an in-memory mock adapter or run this work inline"
    )


# ....................... #


class _NullSelector(selectors.BaseSelector):
    """Selector that performs no I/O and advances the loop's virtual clock on select."""

    def __init__(self, loop: SimulationEventLoop) -> None:
        self._loop = loop
        self._map: dict[Any, selectors.SelectorKey] = {}

    # ....................... #

    def register(
        self,
        fileobj: Any,
        events: int,
        data: Any = None,
    ) -> selectors.SelectorKey:
        raise _forbidden("registering a file descriptor")

    # ....................... #

    def unregister(self, fileobj: Any) -> selectors.SelectorKey:
        raise _forbidden("unregistering a file descriptor")

    # ....................... #

    def select(self, timeout: float | None = None) -> list[Any]:
        # The loop's only timing hook (BaseEventLoop._run_once): ``timeout`` is the
        # delay until the next scheduled callback, 0 when work is ready, or None when
        # nothing is pending at all.
        if timeout is None:
            raise SimulationDeadlock(
                "simulation is quiescent: no ready callbacks and no scheduled timers"
            )

        if timeout > 0:
            self._loop._advance(timeout)  # pyright: ignore[reportPrivateUsage]

        return []

    # ....................... #

    def get_map(self) -> Mapping[Any, selectors.SelectorKey]:
        return self._map

    # ....................... #

    def close(self) -> None:
        self._map = {}


# ....................... #


class SimulationEventLoop(asyncio.BaseEventLoop):
    """Single-threaded asyncio loop with a virtual clock and no real I/O.

    Time starts at ``0.0`` and advances only when the loop waits on a timer, so a
    workload full of ``asyncio.sleep`` / timeouts runs in real-wall milliseconds and
    deterministically. Pair with a simulation :class:`TimeSource` (whose ``monotonic``
    reads ``loop.time()``) so application clocks track the same virtual time.

    Interleaving control (opt-in) reorders the ready-callback queue each tick — the
    continuations concurrent tasks scheduled via ``call_soon`` after awaiting — to explore
    orderings FIFO would never reach (where order-dependent races hide). Pass either
    *schedule_rng* (uniform shuffle each tick) or a *scheduler* object (duck-typed
    ``reorder(ready, step) -> list``; e.g. a :class:`~forze_dst.scheduler.PCTScheduler`) —
    *scheduler* takes precedence. Both are deliberately separate from the application entropy
    seam, so a schedule can be varied without changing application values, and vice versa.
    Same seed → same interleaving, so any bug surfaced reproduces.
    """

    def __init__(
        self,
        *,
        schedule_rng: random.Random | None = None,
        scheduler: Any = None,
    ) -> None:
        super().__init__()
        self._sim_clock: float = 0.0
        self._schedule_rng = schedule_rng
        self._scheduler = scheduler
        self._step = 0
        self._selector: _NullSelector = _NullSelector(self)

    # ....................... #

    def _run_once(self) -> None:
        # Reorder only the callbacks already ready at the start of the tick. BaseEventLoop
        # then runs them (and any timers due this tick) in this order.
        ready = cast("Any", self)._ready  # CPython internal, absent from typeshed

        if self._scheduler is not None:
            if len(ready) > 1:
                self._step += 1
                ordered = self._scheduler.reorder(list(ready), self._step)
                ready.clear()
                ready.extend(ordered)

        elif self._schedule_rng is not None and len(ready) > 1:
            ordered = list(ready)
            self._schedule_rng.shuffle(ordered)
            ready.clear()
            ready.extend(ordered)

        cast("Any", super())._run_once()

    # ....................... #
    # the two virtual-time hooks

    def time(self) -> float:
        return self._sim_clock

    def _advance(self, seconds: float) -> None:
        if seconds > 0:
            self._sim_clock += seconds

    # ....................... #
    # BaseEventLoop concrete-subclass requirements

    def _process_events(self, event_list: list[Any]) -> None:
        # The null selector never yields I/O events; this should always be empty.
        assert not event_list  # nosec B101 - simulation invariant, not a runtime guard

    def _write_to_self(self) -> None:
        # Single-threaded: there is no other thread to wake.
        pass

    def call_soon_threadsafe(self, *args: Any, **kwargs: Any) -> Any:  # type: ignore[override]
        # The simulation is single-threaded and ``_write_to_self`` is a no-op, so a threadsafe
        # wakeup can only come from a real foreign thread — which would inject callbacks whose
        # timing depends on wall-clock thread scheduling, breaking determinism. Refuse it (the
        # in-loop path uses ``call_soon``); make such work inline for simulation.
        raise _forbidden("scheduling a callback from another thread (call_soon_threadsafe)")

    def close(self) -> None:
        self._selector.close()
        super().close()

    # ....................... #
    # real-world escapes are forbidden (leak guard)

    def _make_socket_transport(self, *args: Any, **kwargs: Any) -> Any:
        raise _forbidden("opening a socket")

    def _make_ssl_transport(self, *args: Any, **kwargs: Any) -> Any:
        raise _forbidden("opening a TLS connection")

    def _make_datagram_transport(self, *args: Any, **kwargs: Any) -> Any:
        raise _forbidden("opening a datagram socket")

    def _make_read_pipe_transport(self, *args: Any, **kwargs: Any) -> Any:
        raise _forbidden("opening a pipe")

    def _make_write_pipe_transport(self, *args: Any, **kwargs: Any) -> Any:
        raise _forbidden("opening a pipe")

    def run_in_executor(  # type: ignore[override]  # deliberately incompatible: always raises
        self,
        executor: Any,
        func: Any,
        *args: Any,
    ) -> Any:
        # Threads break determinism — surface it instead of silently running real
        # work off-loop. Make such work inline for simulation.
        raise _forbidden("offloading to a thread/process executor")
