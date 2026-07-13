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

from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

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


@attrs.define(slots=True, frozen=True, kw_only=True)
class PortSelector:
    """Matches port calls by ``surface`` / ``route`` / ``op`` — any field ``None`` matches anything.

    The shared selection value object: rules and interceptors that target a subset of port calls
    (fault, latency, …) carry these three fields and ask :meth:`matches`, rather than each
    re-declaring the triple and its matching logic.
    """

    surface: str | None = None
    """The port surface to match (``None`` = any)."""

    route: str | None = None
    """The port route to match (``None`` = any)."""

    op: str | None = None
    """The port operation to match (``None`` = any)."""

    # ....................... #

    def matches_parts(self, surface: str | None, route: str | None, op: str) -> bool:
        """Whether a call described by *(surface, route, op)* matches this selector."""

        return (
            (self.surface is None or surface == self.surface)
            and (self.route is None or route == self.route)
            and (self.op is None or op == self.op)
        )

    # ....................... #

    def matches(self, call: "PortCall") -> bool:
        """Whether *call* matches this selector."""

        return self.matches_parts(call.surface, call.route, call.op)


# ....................... #


PortNext = Callable[["PortCall"], Awaitable[Any]]
"""Continuation that invokes the rest of the chain (ultimately the real port method)."""


class PortInterceptor(Protocol):
    """A composable middleware around a port call.

    ``around`` may delay, short-circuit (return a synthetic result), raise (inject a fault),
    or call ``nxt(call)`` and post-process its result. It must await ``nxt`` at most once.

    For an **async-generator** port method (a stream — ``find_cursor``, ``search_stream``,
    ``consume``, ``run_chunked``, …) ``around`` sees only the *acquisition* of the iterator:
    ``nxt(call)`` returns the generator, which the proxy iterates afterwards. To act on each
    yielded item (a per-item interleaving / fault-injection point) or across the whole stream
    (duration, mid-stream failure), additionally implement :class:`StreamPortInterceptor`.
    """

    async def around(self, call: PortCall, nxt: PortNext) -> Any: ...


PortInterceptorChain = tuple[PortInterceptor, ...]
"""An ordered interceptor chain; the first element is the outermost."""


# ....................... #


StreamPortNext = Callable[["PortCall"], AsyncIterator[Any]]
"""Continuation for an async-generator port call: returns the rest-of-chain async iterator
(the real port method's generator at the terminal). Called synchronously — iterating it drives
the inner chain — so a :class:`StreamPortInterceptor` awaits *per item*, not on ``nxt`` itself."""


@runtime_checkable
class StreamPortInterceptor(Protocol):
    """Optional capability: interpose on an async-generator port call *as a stream*.

    Where :meth:`PortInterceptor.around` wraps only obtaining the generator, ``around_stream``
    wraps the **iteration**: an implementer ``async for item in nxt(call): ... yield item`` and
    may act at stream open, on each item (delay for a per-item interleaving point, transform,
    or raise to inject a mid-stream fault), and at stream end or failure (``try/except/finally``
    around the loop — e.g. timing the whole stream and logging a mid-stream error). It must
    re-yield the inner items (optionally transformed) unless it deliberately short-circuits.

    The proxy uses ``around_stream`` for async-generator methods when an interceptor implements
    it, and falls back to ``around`` (acquisition only) otherwise — so an ``around``-only
    interceptor keeps its historical behavior. The check is structural (``runtime_checkable``);
    an interceptor may implement one or both.
    """

    def around_stream(self, call: PortCall, nxt: StreamPortNext) -> AsyncIterator[Any]: ...
