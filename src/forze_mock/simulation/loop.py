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

    When *schedule_rng* is given, the ready-callback queue is shuffled with it on
    every tick — perturbing the interleaving of concurrent tasks' continuations to
    explore orderings FIFO would never reach (where order-dependent races hide).
    The RNG is deliberately separate from the application entropy seam, so a schedule
    can be varied without changing application values, and vice versa. Same RNG seed →
    same interleaving, so any bug it surfaces reproduces.
    """

    def __init__(self, *, schedule_rng: random.Random | None = None) -> None:
        super().__init__()
        self._sim_clock: float = 0.0
        self._schedule_rng = schedule_rng
        self._selector: _NullSelector = _NullSelector(self)

    # ....................... #

    def _run_once(self) -> None:
        # Perturb only the callbacks already ready at the start of the tick — the
        # continuations concurrent tasks scheduled via ``call_soon`` after awaiting.
        # BaseEventLoop then runs them (and any timers due this tick) in this order.
        rng = self._schedule_rng
        if rng is not None:
            ready = cast("Any", self)._ready  # CPython internal, absent from typeshed
            if len(ready) > 1:
                ordered = list(ready)
                rng.shuffle(ordered)
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
