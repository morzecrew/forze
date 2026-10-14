"""Where the scope is in its life, and the work it does on its way down.

A lifecycle step cannot hold the runtime that runs it: the runtime is frozen around its
lifecycle plan, so a step built with ``runtime=`` was built before that runtime existed and
cannot sit in its plan. The state a step needs — has startup finished, has a drain been
announced — therefore lives on the execution context the step's hooks already receive, and
so does the registry of drain phases a step contributes to.
"""

import asyncio
from collections.abc import Awaitable, Callable
from contextvars import Context, copy_context
from datetime import timedelta
from typing import TYPE_CHECKING, Final, Literal, final

import attrs

from forze.base.exceptions import exc

if TYPE_CHECKING:
    from .execution import ExecutionContext

# ----------------------- #

Readiness = Literal["ready", "draining", "unavailable"]
"""A scope's readiness: serving, on its way down, or not serving yet."""

DRAIN_PHASE_FLOOR_SECONDS: Final[float] = 0.1
"""A drain phase whose allowance falls below this is skipped rather than started: no
phase does useful work in less, and starting one only to cancel it at once is noise. A
``max_duration`` under it is refused at registration."""


# ....................... #


@final
@attrs.define(slots=True)
class LifecycleState:
    """The scope's lifecycle state (``ctx.lifecycle``).

    Set by :class:`~forze.application.execution.runtime.ExecutionRuntime`; steps read it.
    """

    _started: bool = attrs.field(default=False, init=False)
    _announced: bool = attrs.field(default=False, init=False)
    _started_in: Context | None = attrs.field(default=None, init=False, repr=False)

    # ....................... #

    @property
    def started(self) -> bool:
        """Lifecycle startup finished: every step's startup hook returned.

        The scope as a whole, not one step: which steps are up, and still owe a shutdown,
        is :attr:`ExecutionContext.lifecycle_started
        <forze.application.execution.context.ExecutionContext.lifecycle_started>`.
        """

        return self._started

    # ....................... #

    @property
    def started_in(self) -> Context | None:
        """The context lifecycle startup finished in — the scope's own, with its bindings
        and nobody else's. Drain phases run in a copy of it, whichever task asks for the
        drain. ``None`` until :attr:`started`."""

        return self._started_in

    # ....................... #

    @property
    def announced(self) -> bool:
        """A drain was announced: readiness is withdrawn, operations are still admitted."""

        return self._announced

    # ....................... #

    def mark_started(self) -> None:
        """Record that lifecycle startup finished, and the context it finished in. Called by
        the runtime, on the task that ran startup."""

        self._started = True
        self._started_in = copy_context()

    # ....................... #

    def announce(self) -> bool:
        """Record a drain announcement. Called by the runtime.

        :returns: ``True`` for the call that announced, ``False`` once already announced.
        """

        if self._announced:
            return False

        self._announced = True

        return True


# ....................... #


DrainPhaseFn = Callable[[timedelta], Awaitable[None]]
"""A drain phase: called with the time it may take, which it should shrink its own work to."""


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class DrainPhase:
    """One registered drain phase."""

    name: str
    """Names the phase in logs."""

    run: DrainPhaseFn
    """The phase itself."""

    order: int = 0
    """Phases run in ascending order; equal orders run in registration order."""

    max_duration: timedelta | None = None
    """This phase's own cap. ``None`` lets it take whatever is left of the drain budget."""


# ....................... #


@final
@attrs.define(slots=True)
class DrainPhases:
    """Work that must happen while the replica still serves (``ctx.drain_phases``).

    A step registers a phase at startup — a courier handing its routes back, sockets closing
    gently over a window — and :meth:`ExecutionRuntime.drain
    <forze.application.execution.runtime.ExecutionRuntime.drain>` runs every phase once,
    after readiness is withdrawn and before the drain gate closes and teardown starts.
    Phases are not drainables: those are loops stopped *after* the gate closes.
    """

    _phases: list[DrainPhase] = attrs.field(factory=list, init=False, repr=False)

    _finished: asyncio.Event | None = attrs.field(default=None, init=False, repr=False)
    """Created when the scope's one drain is claimed, set when it finishes."""

    # ....................... #

    def register(
        self,
        name: str,
        phase: DrainPhaseFn,
        *,
        order: int = 0,
        max_duration: timedelta | None = None,
    ) -> None:
        """Register *phase* to run when the scope drains.

        :param name: Names the phase in logs; unique within the scope.
        :param phase: Awaited with the time it may take: ``min(max_duration, budget left)``.
            Past that it is cancelled.
        :param order: Phases run in ascending order.
        :param max_duration: This phase's cap. Give early phases one: without it a slow
            early phase may spend the whole budget and leave the later ones nothing.

        :raises CoreException: ``configuration`` — the name is taken, or *max_duration* is
            under the 100 ms floor (:data:`DRAIN_PHASE_FLOOR_SECONDS`) a phase is skipped
            below.
        """

        if max_duration is not None and max_duration.total_seconds() < DRAIN_PHASE_FLOOR_SECONDS:
            # It would be skipped on every drain: refused here, where the mistake is made.
            raise exc.configuration(
                f"Drain phase {name!r} needs a max_duration of at least "
                f"{DRAIN_PHASE_FLOOR_SECONDS}s, got {max_duration}"
            )

        if any(one.name == name for one in self._phases):
            raise exc.configuration(f"Drain phase {name!r} is already registered")

        self._phases.append(
            DrainPhase(name=name, run=phase, order=order, max_duration=max_duration)
        )

    # ....................... #

    @property
    def phases(self) -> tuple[DrainPhase, ...]:
        """Every registered phase, in the order a drain runs them."""

        return tuple(sorted(self._phases, key=lambda one: one.order))

    # ....................... #

    def claim(self) -> bool:
        """Claim the scope's one drain run. Called by the runtime.

        :returns: ``True`` for the caller that runs it; ``False`` once a drain was claimed.
        """

        if self._finished is not None:
            return False

        self._finished = asyncio.Event()

        return True

    # ....................... #

    def finish(self) -> None:
        """Mark the claimed drain finished. Called by the runtime."""

        if self._finished is not None:
            self._finished.set()

    # ....................... #

    async def wait(self) -> None:
        """Wait until a claimed drain finished; returns at once when none was claimed."""

        if self._finished is not None:
            await self._finished.wait()


# ....................... #


def scope_readiness(ctx: "ExecutionContext") -> Readiness:
    """The one readiness rule, shared by the runtime's properties and every probe.

    ``draining`` once a drain was announced or the drain gate closed, ``ready`` once startup
    finished, ``unavailable`` before that.
    """

    if ctx.lifecycle.announced or ctx.drain_gate.draining:
        return "draining"

    return "ready" if ctx.lifecycle.started else "unavailable"
