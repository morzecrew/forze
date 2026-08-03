"""Armed faults and latency, applied at the port seam."""

from __future__ import annotations

import asyncio
from typing import Any, final

import attrs

from forze.application.contracts.interception import PortCall, PortNext, PortSelector
from forze.base.exceptions import CoreException, ExceptionKind, exc

# ----------------------- #


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
                return fault

            if fault.remaining <= 1:
                del self.faults[index]

            else:
                self.faults[index] = attrs.evolve(fault, remaining=fault.remaining - 1)

            return fault

        return None


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
