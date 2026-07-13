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

Async-generator methods intercept the acquisition of the generator via ``around``; an
interceptor that additionally implements :class:`StreamPortInterceptor` (``around_stream``)
also wraps the *iteration*, so it can yield per item (a per-item interleaving point), inject a
mid-stream fault, and time or log the whole stream. An ``around``-only interceptor keeps the
acquisition-only behavior (see
:class:`~forze.application.execution.interception.proxy.InterceptingPortProxy`).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

# The interception seam itself (PortCall value object + PortInterceptor protocol) is a
# contract; this module adds the run-scoped ambient binding (execution machinery) and
# re-exports the contract types so ``from .protocol import PortInterceptor`` holds.
from forze.application.contracts.interception import (
    PortCall,
    PortInterceptor,
    PortInterceptorChain,
    PortNext,
    PortSelector,
    StreamPortInterceptor,
    StreamPortNext,
)

__all__ = [
    "PortCall",
    "PortInterceptor",
    "PortInterceptorChain",
    "PortNext",
    "PortSelector",
    "StreamPortInterceptor",
    "StreamPortNext",
    "bind_interceptors",
    "current_interceptors",
]

# ----------------------- #


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
