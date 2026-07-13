from collections.abc import AsyncGenerator, Awaitable, Callable, Generator, Mapping
from contextlib import (
    AbstractAsyncContextManager,
    AbstractContextManager,
    aclosing,
    closing,
)
from functools import wraps
from typing import (
    Any,
    Never,
    ParamSpec,
    TypeVar,
    final,
)

import attrs

from ._intercept import (
    AsyncContextManagerExceptionInterceptor,
    ContextManagerExceptionInterceptor,
    materialize_bound_details,
)
from ._utils import BYPASS_INTERCEPTION, reraise_mapped, resolve_site
from .model import CoreException
from .protocols import ExceptionMapper

# ----------------------- #

P = ParamSpec("P")
R = TypeVar("R")

_BYPASS_INTERCEPTION = BYPASS_INTERCEPTION

_PASSTHROUGH: tuple[type[BaseException], ...] = (CoreException, *_BYPASS_INTERCEPTION)
"""Exceptions reraised as-is without materializing error details."""

# ....................... #


def _reraise_lazy(
    mapper: ExceptionMapper,
    exc: BaseException,
    *,
    site: str,
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> Never:
    """Reraise a mapped exception, materializing bound-arg details lazily.

    Control-flow exceptions (:data:`BYPASS_INTERCEPTION`) and
    :class:`~forze.base.exceptions.model.CoreException` pass through without
    paying any detail-materialization cost.
    """

    if isinstance(exc, _PASSTHROUGH):
        raise exc

    try:
        details = materialize_bound_details(fn, args, kwargs)

    except Exception:  # never mask the original error during materialization
        details = None

    reraise_mapped(mapper, exc, site=site, details=details)


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class ExceptionInterceptor:
    """Exception interceptor.

    Error-context details (sanitized bound arguments) are materialized lazily,
    only when a mappable exception is intercepted — the success path pays no
    signature-binding or sanitization cost. As a consequence, the details
    attached to mapped exceptions reflect argument state at *failure* time
    rather than call time: arguments mutated by the callee may differ.
    """

    mapper: ExceptionMapper
    """The mapper to use for intercepted exceptions."""

    # ....................... #

    def coroutine(
        self,
        site: str | None = None,
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
        """Wrap a coroutine function to intercept exceptions."""

        def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
            resolved_site = resolve_site(fn, site)

            @wraps(fn)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                try:
                    return await fn(*args, **kwargs)

                except BaseException as e:
                    _reraise_lazy(
                        self.mapper,
                        e,
                        site=resolved_site,
                        fn=fn,
                        args=args,
                        kwargs=kwargs,
                    )

            return wrapper

        return decorator

    # ....................... #

    def function(
        self,
        site: str | None = None,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """Wrap a sync function to intercept exceptions."""

        def decorator(fn: Callable[P, R]) -> Callable[P, R]:
            resolved_site = resolve_site(fn, site)

            @wraps(fn)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                try:
                    return fn(*args, **kwargs)

                except BaseException as e:
                    _reraise_lazy(
                        self.mapper,
                        e,
                        site=resolved_site,
                        fn=fn,
                        args=args,
                        kwargs=kwargs,
                    )

            return wrapper

        return decorator

    # ....................... #

    def asyncgenerator(
        self,
        site: str | None = None,
    ) -> Callable[[Callable[P, AsyncGenerator[R]]], Callable[P, AsyncGenerator[R]]]:
        """Wrap an async generator function to intercept exceptions."""

        def decorator(
            fn: Callable[P, AsyncGenerator[R]],
        ) -> Callable[P, AsyncGenerator[R]]:
            resolved_site = resolve_site(fn, site)

            @wraps(fn)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> AsyncGenerator[R]:
                try:
                    it = fn(*args, **kwargs)

                except BaseException as e:
                    _reraise_lazy(
                        self.mapper,
                        e,
                        site=resolved_site,
                        fn=fn,
                        args=args,
                        kwargs=kwargs,
                    )

                async with aclosing(it) as agen:
                    async for x in agen:
                        try:
                            yield x

                        except BaseException as e:
                            _reraise_lazy(
                                self.mapper,
                                e,
                                site=resolved_site,
                                fn=fn,
                                args=args,
                                kwargs=kwargs,
                            )

            return wrapper

        return decorator

    # ....................... #

    def generator(
        self,
        site: str | None = None,
    ) -> Callable[[Callable[P, Generator[R]]], Callable[P, Generator[R]]]:
        """Wrap a generator function to intercept exceptions."""

        def decorator(fn: Callable[P, Generator[R]]) -> Callable[P, Generator[R]]:
            resolved_site = resolve_site(fn, site)

            @wraps(fn)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> Generator[R]:
                try:
                    it = fn(*args, **kwargs)

                except BaseException as e:
                    _reraise_lazy(
                        self.mapper,
                        e,
                        site=resolved_site,
                        fn=fn,
                        args=args,
                        kwargs=kwargs,
                    )

                with closing(it) as gen:
                    for x in gen:
                        try:
                            yield x

                        except BaseException as e:
                            _reraise_lazy(
                                self.mapper,
                                e,
                                site=resolved_site,
                                fn=fn,
                                args=args,
                                kwargs=kwargs,
                            )

            return wrapper

        return decorator

    # ....................... #

    def contextmanager(
        self,
        site: str | None = None,
    ) -> Callable[[Callable[P, AbstractContextManager[R]]], Callable[P, AbstractContextManager[R]]]:
        """Wrap a context manager function to intercept exceptions."""

        def decorator(
            fn: Callable[P, AbstractContextManager[R]],
        ) -> Callable[P, AbstractContextManager[R]]:
            resolved_site = resolve_site(fn, site)

            @wraps(fn)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> AbstractContextManager[R]:
                try:
                    cm = fn(*args, **kwargs)

                except BaseException as e:
                    _reraise_lazy(
                        self.mapper,
                        e,
                        site=resolved_site,
                        fn=fn,
                        args=args,
                        kwargs=kwargs,
                    )

                return ContextManagerExceptionInterceptor(
                    cm=cm,
                    mapper=self.mapper,
                    site=resolved_site,
                    details_factory=lambda: materialize_bound_details(fn, args, kwargs),
                )

            return wrapper

        return decorator

    # ....................... #

    def asynccontextmanager(
        self,
        site: str | None = None,
    ) -> Callable[
        [Callable[P, AbstractAsyncContextManager[R]]],
        Callable[P, AbstractAsyncContextManager[R]],
    ]:
        """Wrap an async context manager function to intercept exceptions."""

        def decorator(
            fn: Callable[P, AbstractAsyncContextManager[R]],
        ) -> Callable[P, AbstractAsyncContextManager[R]]:
            resolved_site = resolve_site(fn, site)

            @wraps(fn)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> AbstractAsyncContextManager[R]:
                try:
                    cm = fn(*args, **kwargs)

                except BaseException as e:
                    _reraise_lazy(
                        self.mapper,
                        e,
                        site=resolved_site,
                        fn=fn,
                        args=args,
                        kwargs=kwargs,
                    )

                return AsyncContextManagerExceptionInterceptor(
                    cm=cm,
                    mapper=self.mapper,
                    site=resolved_site,
                    details_factory=lambda: materialize_bound_details(fn, args, kwargs),
                )

            return wrapper

        return decorator
