"""Where the scope is in its life, readable from any lifecycle hook's ``ctx``.

A lifecycle step cannot hold the runtime that runs it: the runtime is frozen around its
lifecycle plan, so a step built with ``runtime=`` was built before that runtime existed and
cannot sit in its plan. The state a step needs — has startup finished, has a drain been
announced — therefore lives on the execution context the step's hooks already receive.
"""

from typing import TYPE_CHECKING, Literal, final

import attrs

if TYPE_CHECKING:
    from .execution import ExecutionContext

# ----------------------- #

Readiness = Literal["ready", "draining", "unavailable"]
"""A scope's readiness: serving, on its way down, or not serving yet."""


# ....................... #


@final
@attrs.define(slots=True)
class LifecycleState:
    """The scope's lifecycle state (``ctx.lifecycle``).

    Set by :class:`~forze.application.execution.runtime.ExecutionRuntime`; steps read it.
    """

    _started: bool = attrs.field(default=False, init=False)
    _announced: bool = attrs.field(default=False, init=False)

    # ....................... #

    @property
    def started(self) -> bool:
        """Lifecycle startup finished: every step's startup hook returned."""

        return self._started

    # ....................... #

    @property
    def announced(self) -> bool:
        """A drain was announced: readiness is withdrawn, operations are still admitted."""

        return self._announced

    # ....................... #

    def mark_started(self) -> None:
        """Record that lifecycle startup finished. Called by the runtime."""

        self._started = True


# ....................... #


def scope_readiness(ctx: "ExecutionContext") -> Readiness:
    """The one readiness rule, shared by the runtime's properties and every probe.

    ``draining`` once a drain was announced or the drain gate closed, ``ready`` once startup
    finished, ``unavailable`` before that.
    """

    if ctx.lifecycle.announced or ctx.drain_gate.draining:
        return "draining"

    return "ready" if ctx.lifecycle.started else "unavailable"
