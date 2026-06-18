"""Port interception seam: a public, composable middleware chain around port calls.

A resolved configurable port can be wrapped so each async (and async-generator) method call
passes through an ordered chain of :class:`PortInterceptor` s before reaching the real
adapter. Production registers none (zero cost); simulation registers interceptors for
cooperative yielding, I/O latency, and fault injection — at the seam, never in handlers.

Two registration surfaces feed the same chain:

* **deps-scoped** — :meth:`~forze.application.execution.deps.registry.DepsRegistry.with_interceptors`
  (app / harness wiring), carried to the resolved port at wrap time; and
* **ambient / run-scoped** — :func:`bind_interceptors` (a ContextVar), for run-scoped drivers
  like ``run_simulation`` that do not own the deps registry.

The effective chain for a call is ``deps_interceptors + ambient_interceptors`` (ambient runs
innermost, closest to the port — it models the I/O boundary). The chain sits **inside** the
resilience port-policy wrap, so a fault interceptor's transient error is retryable.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Awaitable, Callable, Iterator, Protocol

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


# ....................... #


_AMBIENT: ContextVar[PortInterceptorChain] = ContextVar(
    "forze_ambient_port_interceptors", default=()
)
"""Run-scoped interceptor chain (empty outside a binding)."""


def current_interceptors() -> PortInterceptorChain:
    """Return the ambient (run-scoped) interceptor chain, or an empty tuple."""

    return _AMBIENT.get()


@contextmanager
def bind_interceptors(*interceptors: PortInterceptor) -> Iterator[None]:
    """Bind *interceptors* as the ambient (run-scoped) chain for the duration."""

    token = _AMBIENT.set(tuple(interceptors))

    try:
        yield

    finally:
        _AMBIENT.reset(token)
