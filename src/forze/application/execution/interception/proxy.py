"""Apply a :class:`PortInterceptor` chain around a resolved port's method calls."""

from __future__ import annotations

from functools import wraps
from typing import Any, cast

import attrs

from ..port_proxy_base import PortProxy
from .protocol import (
    PortCall,
    PortInterceptor,
    PortInterceptorChain,
    PortNext,
    current_interceptors,
)

# ----------------------- #


async def run_chain(
    interceptors: PortInterceptorChain,
    call: PortCall,
    terminal: PortNext,
) -> Any:
    """Compose *interceptors* (first = outermost) around *terminal* and run them for *call*."""

    handler: PortNext = terminal

    for interceptor in reversed(interceptors):
        nxt = handler

        async def step(
            c: PortCall,
            _i: PortInterceptor = interceptor,
            _n: PortNext = nxt,
        ) -> Any:
            return await _i.around(c, _n)

        handler = step

    return await handler(call)


# ....................... #


@attrs.define(slots=True)
class InterceptingPortProxy(PortProxy):
    """Wrap a port so each async / async-gen method call runs through the interceptor chain.

    Sync methods pass through untouched (the base default) — interceptors model I/O
    boundaries, which are async, matching the prior cooperative-yield behavior. The
    effective chain per call is the deps-scoped interceptors fixed at wrap time plus the
    ambient chain read per call (ambient innermost).

    **Async-generator limitation.** For an async-generator method the chain wraps only
    *obtaining* the generator (:meth:`_wrap_async_gen`); the subsequent per-item iteration
    runs *outside* the chain. So an interceptor sees one ``around`` at open, not one per
    yielded item: a ``LoggingInterceptor`` records the open (duration ≈ 0, "success") even if
    the stream later fails mid-iteration; a DST cooperative-yield interceptor yields once at
    open, not between items; and a fault interceptor cannot inject a mid-stream fault. This is
    a deliberate consequence of the request/response ``around(call, next)`` shape — a proper
    per-item hook needs a stream-aware interceptor method, not yet part of the contract. Treat
    streamed reads as a single interception point.
    """

    interceptors: PortInterceptorChain
    """The interceptor chain to use."""

    surface: str | None
    """The surface to use."""

    route: str | None
    """The route to use."""

    # ....................... #

    def _chain(self) -> PortInterceptorChain:
        ambient = current_interceptors()

        return (*self.interceptors, *ambient) if ambient else self.interceptors

    # ....................... #

    def _wrap_async_gen(self, name: str, attr: Any) -> Any:
        # The terminal closes over only ``attr`` (fixed for this method), so build it once
        # at wrap time rather than allocating a fresh closure on every call.
        async def terminal(c: PortCall) -> Any:
            # Honor the (possibly interceptor-rewritten) call, not the original args.
            return attr(*c.args, **c.kwargs)

        @wraps(attr)
        async def intercepted_async_gen(*args: Any, **kwargs: Any) -> Any:
            call = PortCall(
                surface=self.surface,
                route=self.route,
                op=name,
                args=args,
                kwargs=kwargs,
            )

            gen = await run_chain(self._chain(), call, terminal)

            async for item in gen:
                yield item

        return intercepted_async_gen

    # ....................... #

    def _wrap_async(self, name: str, attr: Any) -> Any:
        # The terminal closes over only ``attr`` (fixed for this method), so build it once
        # at wrap time rather than allocating a fresh closure on every call.
        async def terminal(c: PortCall) -> Any:
            # Honor the (possibly interceptor-rewritten) call, not the original args.
            return await attr(*c.args, **c.kwargs)

        @wraps(attr)
        async def intercepted_async(*args: Any, **kwargs: Any) -> Any:
            call = PortCall(
                surface=self.surface,
                route=self.route,
                op=name,
                args=args,
                kwargs=kwargs,
            )

            return await run_chain(self._chain(), call, terminal)

        return intercepted_async


# ....................... #


def wrap_intercepted[T](
    inner: T,
    *,
    interceptors: PortInterceptorChain,
    surface: str | None,
    route: str | None,
) -> T:
    """Return *inner* wrapped to run the interceptor chain around its async calls."""

    return cast(
        T,
        InterceptingPortProxy(
            inner=inner,
            interceptors=interceptors,
            surface=surface,
            route=route,
        ),
    )
