"""Armed faults and latency, applied at the port seam."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, final

import attrs

from forze.application.contracts.interception import PortCall, PortNext, PortSelector
from forze.base.exceptions import CoreException, ExceptionKind, exc

# ----------------------- #

_fired_here: ContextVar[list[int] | None] = ContextVar("forze_mock_faults_fired", default=None)
"""Faults consumed by *this* task, when someone is counting."""

# ....................... #


@contextmanager
def counting_fired() -> Iterator[Callable[[], int]]:
    """Count the faults consumed by the work done inside this block, and nothing else.

    The board is shared by every request, so its running total cannot answer "did *my* call
    fail because of an injection?" — a fault fired by a concurrent request lands in the same
    number. A ContextVar scopes the tally to the calling task: awaited work inherits it,
    other requests each carry their own, and the answer stops depending on who else is busy.
    """

    tally = [0]
    token = _fired_here.set(tally)

    try:
        yield lambda: tally[0]

    finally:
        _fired_here.reset(token)


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ArmedFault:
    """A failure waiting for the call that matches it."""

    selector: PortSelector
    """Which port calls it applies to (``surface``/``route``/``op``, ``None`` = any)."""

    kind: ExceptionKind
    """A **real** exception kind. The app's own mapping turns it into the real status and
    error envelope, so an armed ``conflict`` reaches the client exactly as a genuine
    optimistic-concurrency failure would — a control plane that raised something bespoke
    would be teaching the frontend a lie."""

    summary: str = "Injected by the mock control plane"
    code: str | None = None
    remaining: int | None = None
    """How many more matching calls it fires on; ``None`` = until disarmed."""

    # ....................... #

    def error(self) -> CoreException:
        """The exception this fault raises."""

        return exc.of(self.kind, self.summary, code=self.code)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ArmedLatency:
    """A delay applied before a matching call reaches the adapter."""

    selector: PortSelector
    seconds: float


# ....................... #


@final
@attrs.define(slots=True)
class FaultBoard:
    """What is currently armed. Mutable and shared: the control plane edits it live."""

    faults: list[ArmedFault] = attrs.field(factory=list)
    latencies: list[ArmedLatency] = attrs.field(factory=list)

    fired: int = attrs.field(default=0, init=False)
    """How many faults this board has ever handed out, across every request.

    What is armed cannot answer "did *that* call fail because of an injection?" — a one-shot
    fault is consumed and removed before the exception it produces reaches the caller, and a
    fault armed elsewhere stays armed through a failure it had nothing to do with. This total
    is the honest observation of the board as a whole; attributing a firing to one call is
    what :func:`counting_fired` is for. Never reset, including by :meth:`clear`."""

    # ....................... #

    def arm_fault(self, fault: ArmedFault) -> None:
        self.faults.append(fault)

    def arm_latency(self, latency: ArmedLatency) -> None:
        self.latencies.append(latency)

    def clear(self) -> None:
        """Disarm everything — what ``POST /_mock/reset`` calls."""

        self.faults.clear()
        self.latencies.clear()

    # ....................... #

    def delay_for(self, call: PortCall) -> float:
        """Total armed delay for *call* (delays compose; a call can match several rules)."""

        return sum(armed.seconds for armed in self.latencies if armed.selector.matches(call))

    # ....................... #

    def take_fault(self, call: PortCall) -> ArmedFault | None:
        """The first fault matching *call*, consuming one of its firings."""

        for index, fault in enumerate(self.faults):
            if not fault.selector.matches(call):
                continue

            if fault.remaining is None:
                self._record_firing()

                return fault

            # A count already at zero is spent, not owed one more: `remaining` says how many
            # firings are *left*, so honouring one here would fire a fault the board reports
            # as having none.
            if fault.remaining <= 0:
                del self.faults[index]

                return self.take_fault(call)

            if fault.remaining == 1:
                del self.faults[index]

            else:
                self.faults[index] = attrs.evolve(fault, remaining=fault.remaining - 1)

            self._record_firing()

            return fault

        return None

    # ....................... #

    def _record_firing(self) -> None:
        """Count one consumed fault, on the board and for whoever is counting this task."""

        self.fired += 1
        tally = _fired_here.get()

        if tally is not None:
            tally[0] += 1


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ControlInterceptor:
    """Applies the board to every intercepted port call.

    Registered deps-scoped, so it wraps every configurable port the app resolves and nothing
    in the app or its handlers knows it exists — fault injection stays at the seam.
    """

    board: FaultBoard

    # ....................... #

    async def around(self, call: PortCall, nxt: PortNext) -> Any:
        delay = self.board.delay_for(call)

        if delay > 0:
            await asyncio.sleep(delay)

        fault = self.board.take_fault(call)

        if fault is not None:
            raise fault.error()

        return await nxt(call)
