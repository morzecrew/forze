"""Port interception contract: the seam an interceptor implements.

A resolved configurable port can be wrapped so each async (and async-generator) method
call passes through an ordered chain of :class:`PortInterceptor` s before reaching the
real adapter — used by simulation for cooperative yielding, I/O latency, and fault
injection, at the seam rather than in handlers.

This module is the contract (the ``PortCall`` value object + the ``PortInterceptor``
protocol and its aliases) so an implementer depends only on contracts; the run-scoped
ambient binding and the wrapping proxy are execution machinery
(``forze.application.execution.interception``).
"""

from typing import Any, Awaitable, Callable, Protocol

import attrs

# ----------------------- #


@attrs.define(slots=True, frozen=True, kw_only=True)
class PortCall:
    """One intercepted port method call."""

    surface: str | None
    """Dependency surface name (for example ``document_command``)."""

    route: str | None
    """Spec route or transaction route name."""

    op: str
    """Method name being called (for example ``create``, ``get``)."""

    args: tuple[Any, ...] = ()
    """Positional arguments passed to the method."""

    kwargs: dict[str, Any] = attrs.field(factory=dict)
    """Keyword arguments passed to the method."""


# ....................... #


PortNext = Callable[["PortCall"], Awaitable[Any]]
"""Continuation that invokes the rest of the chain (ultimately the real port method)."""


class PortInterceptor(Protocol):
    """A composable middleware around a port call.

    ``around`` may delay, short-circuit (return a synthetic result), raise (inject a fault),
    or call ``nxt(call)`` and post-process its result. It must await ``nxt`` at most once.
    """

    async def around(self, call: PortCall, nxt: PortNext) -> Any: ...


PortInterceptorChain = tuple[PortInterceptor, ...]
"""An ordered interceptor chain; the first element is the outermost."""
