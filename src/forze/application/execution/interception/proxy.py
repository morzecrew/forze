"""Apply a :class:`PortInterceptor` chain around a resolved port's method calls."""

from __future__ import annotations

import inspect
from functools import wraps
from typing import Any, cast

import attrs

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
class InterceptingPortProxy:
    """Wrap a port so each async / async-gen method call runs through the interceptor chain.

    Sync methods pass through untouched — interceptors model I/O boundaries (which are
    async), matching the prior cooperative-yield behavior. The effective chain per call is
    the deps-scoped interceptors fixed at wrap time plus the ambient chain read per call
    (ambient innermost).
    """

    inner: Any
    """The port to wrap."""

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

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self.inner, name)

        if not callable(attr):
            return attr

        if inspect.isasyncgenfunction(attr):

            @wraps(attr)
            async def intercepted_async_gen(*args: Any, **kwargs: Any) -> Any:
                call = PortCall(
                    surface=self.surface,
                    route=self.route,
                    op=name,
                    args=args,
                    kwargs=kwargs,
                )

                async def terminal(c: PortCall) -> Any:
                    # Honor the (possibly interceptor-rewritten) call, not the original args.
                    return attr(*c.args, **c.kwargs)

                gen = await run_chain(self._chain(), call, terminal)

                async for item in gen:
                    yield item

            return intercepted_async_gen

        if inspect.iscoroutinefunction(attr):

            @wraps(attr)
            async def intercepted_async(*args: Any, **kwargs: Any) -> Any:
                call = PortCall(
                    surface=self.surface,
                    route=self.route,
                    op=name,
                    args=args,
                    kwargs=kwargs,
                )

                async def terminal(c: PortCall) -> Any:
                    # Honor the (possibly interceptor-rewritten) call, not the original args.
                    return await attr(*c.args, **c.kwargs)

                return await run_chain(self._chain(), call, terminal)

            return intercepted_async

        return (
            attr  # sync method: pass through (matches prior cooperative-yield behavior)
        )


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
