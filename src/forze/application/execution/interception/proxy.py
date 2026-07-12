"""Apply a :class:`PortInterceptor` chain around a resolved port's method calls."""

from __future__ import annotations

from contextlib import aclosing
from functools import wraps
from typing import Any, AsyncGenerator, AsyncIterator, cast

import attrs

from ..port_proxy_base import PortProxy
from .protocol import (
    PortCall,
    PortInterceptor,
    PortInterceptorChain,
    PortNext,
    StreamPortInterceptor,
    StreamPortNext,
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


async def _acquisition_only_stream(
    interceptor: PortInterceptor, call: PortCall, nxt: StreamPortNext
) -> AsyncIterator[Any]:
    """Adapt an ``around``-only interceptor to a stream: intercept *acquisition* only.

    Runs the interceptor's ``around`` with the async iterator as the call's result (obtained,
    not iterated), preserving the historical behavior for an interceptor that does not
    implement :class:`StreamPortInterceptor`, then iterates whatever it returned.
    """

    async def acquire(c: PortCall) -> Any:
        return nxt(c)  # the (rest-of-chain) async iterator — a value, not yet iterated

    stream = await interceptor.around(call, acquire)

    # Close the inner stream deterministically on any exit (early break, exception) so a
    # backend cursor/connection is released promptly rather than at GC time — seam streams
    # are async generators (they have ``aclose``); the annotated ``AsyncIterator`` is narrowed.
    async with aclosing(cast("AsyncGenerator[Any, None]", stream)) as agen:
        async for item in agen:
            yield item


def compose_stream_chain(
    interceptors: PortInterceptorChain, terminal: StreamPortNext
) -> StreamPortNext:
    """Compose *interceptors* (first = outermost) into a single async-iterator continuation.

    A :class:`StreamPortInterceptor` wraps the iteration via ``around_stream``; any other
    interceptor wraps only acquisition (:func:`_acquisition_only_stream`).
    """

    handler: StreamPortNext = terminal

    for interceptor in reversed(interceptors):
        nxt = handler
        stream_aware = isinstance(interceptor, StreamPortInterceptor)

        def step(
            c: PortCall,
            _i: Any = interceptor,
            _n: StreamPortNext = nxt,
            _stream_aware: bool = stream_aware,
        ) -> AsyncIterator[Any]:
            if _stream_aware:
                return _i.around_stream(c, _n)

            return _acquisition_only_stream(_i, c, _n)

        handler = step

    return handler


# ....................... #


@attrs.define(slots=True)
class InterceptingPortProxy(PortProxy):
    """Wrap a port so each async / async-gen method call runs through the interceptor chain.

    Sync methods pass through untouched (the base default) — interceptors model I/O
    boundaries, which are async, matching the prior cooperative-yield behavior. The
    effective chain per call is the deps-scoped interceptors fixed at wrap time plus the
    ambient chain read per call (ambient innermost).

    **Async generators.** For an async-generator method the chain wraps iteration through
    :func:`compose_stream_chain`: an interceptor implementing
    :class:`~forze.application.contracts.interception.StreamPortInterceptor` (``around_stream``)
    acts per item and across the whole stream (a per-item interleaving point, a mid-stream
    fault, stream-duration logging), while an ``around``-only interceptor keeps the historical
    acquisition-only behavior (:func:`_acquisition_only_stream`).
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
        # at wrap time rather than allocating a fresh closure on every call. It returns the
        # generator synchronously (calling an async-gen function does not iterate it).
        def terminal(c: PortCall) -> AsyncIterator[Any]:
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

            # Compose per call: the ambient chain is read per call (see ``_chain``).
            stream = compose_stream_chain(self._chain(), terminal)

            # Close the composed stream deterministically on any exit (consumer ``aclose``,
            # early break, a thrown-in exception) so the close chains down to the backend
            # generator at scope exit rather than at GC time.
            async with aclosing(
                cast("AsyncGenerator[Any, None]", stream(call))
            ) as agen:
                async for item in agen:
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
