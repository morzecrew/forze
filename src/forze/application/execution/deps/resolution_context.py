"""Per-container dependency resolution stack and cycle detection."""

from contextvars import ContextVar, Token
from typing import final

import attrs

from forze.base.exceptions import exc

from .resolution import ResolutionFrame, format_cycle_error
from .resolution_tracer import ResolutionTracer

# ----------------------- #


@final
@attrs.define(slots=True)
class ResolutionContext:
    """Per-task resolution stack for one dependency container."""

    tracer: ResolutionTracer
    """Optional recorder for observed resolution edges."""

    # ....................... #

    _stack: ContextVar[tuple[ResolutionFrame, ...]] = attrs.field(
        factory=lambda: ContextVar("deps_resolution_stack", default=()),
        init=False,
        repr=False,
        eq=False,
        hash=False,
    )

    # ....................... #

    def stack(self) -> tuple[ResolutionFrame, ...]:
        return self._stack.get()

    # ....................... #

    def push(self, frame: ResolutionFrame) -> Token[tuple[ResolutionFrame, ...]]:
        stack = self.stack()

        if frame in stack:
            raise exc.internal(format_cycle_error(stack, frame))

        if stack:
            self.tracer.record_edge(stack[-1], frame)

        return self._stack.set((*stack, frame))

    # ....................... #

    def pop(self, token: Token[tuple[ResolutionFrame, ...]]) -> None:
        self._stack.reset(token)

    # ....................... #

    def assert_not_active(self, frame: ResolutionFrame) -> None:
        stack = self.stack()

        if frame in stack:
            raise exc.internal(format_cycle_error(stack, frame))

    # ....................... #

    def record_provide_edge(self, child: ResolutionFrame) -> None:
        """Record an edge from the active frame to *child* without pushing."""

        if stack := self.stack():
            self.tracer.record_edge(stack[-1], child)
