from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import wraps
from typing import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    ParamSpec,
    TypeVar,
    final,
)

import attrs

from ._intercept import (
    AsyncContextManagerExceptionInterceptor,
    ContextManagerExceptionInterceptor,
    Intercepted,
)
from ._utils import reraise_mapped
from .protocols import ExceptionMapper

# ----------------------- #

P = ParamSpec("P")
R = TypeVar("R")

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class ExceptionInterceptor:
    """Exception interceptor."""

    mapper: ExceptionMapper
    """The mapper to use for intercepted exceptions."""

    # ....................... #

    def coroutine(
        self,
        site: str | None = None,
    ) -> Callable[[Callable[P, Awaitable[R]]], Callable[P, Awaitable[R]]]:
        """Wrap a coroutine function to intercept exceptions."""

        def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:

            @wraps(fn)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                intercepted = Intercepted.from_callable(fn, *args, site=site, **kwargs)

                try:
                    return await fn(*args, **kwargs)

                except BaseException as e:
                    reraise_mapped(
                        self.mapper,
                        e,
                        site=intercepted.site,
                        details=intercepted.bound,
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

            @wraps(fn)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                intercepted = Intercepted.from_callable(fn, *args, site=site, **kwargs)

                try:
                    return fn(*args, **kwargs)

                except BaseException as e:
                    reraise_mapped(
                        self.mapper,
                        e,
                        site=intercepted.site,
                        details=intercepted.bound,
                    )

            return wrapper

        return decorator

    # ....................... #

    def asyncgenerator(
        self,
        site: str | None = None,
    ) -> Callable[[Callable[P, AsyncIterator[R]]], Callable[P, AsyncIterator[R]]]:
        """Wrap an async generator function to intercept exceptions."""

        def decorator(
            fn: Callable[P, AsyncIterator[R]],
        ) -> Callable[P, AsyncIterator[R]]:
            @wraps(fn)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> AsyncIterator[R]:
                intercepted = Intercepted.from_callable(fn, *args, site=site, **kwargs)

                try:
                    it = fn(*args, **kwargs)

                except BaseException as e:
                    reraise_mapped(
                        self.mapper,
                        e,
                        site=intercepted.site,
                        details=intercepted.bound,
                    )

                async for x in it:
                    try:
                        yield x

                    except BaseException as e:
                        reraise_mapped(
                            self.mapper,
                            e,
                            site=intercepted.site,
                            details=intercepted.bound,
                        )

            return wrapper

        return decorator

    # ....................... #

    def generator(
        self,
        site: str | None = None,
    ) -> Callable[[Callable[P, Iterator[R]]], Callable[P, Iterator[R]]]:
        """Wrap a generator function to intercept exceptions."""

        def decorator(fn: Callable[P, Iterator[R]]) -> Callable[P, Iterator[R]]:
            @wraps(fn)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> Iterator[R]:
                intercepted = Intercepted.from_callable(fn, *args, site=site, **kwargs)

                try:
                    it = fn(*args, **kwargs)

                except BaseException as e:
                    reraise_mapped(
                        self.mapper,
                        e,
                        site=intercepted.site,
                        details=intercepted.bound,
                    )

                for x in it:
                    try:
                        yield x

                    except BaseException as e:
                        reraise_mapped(
                            self.mapper,
                            e,
                            site=intercepted.site,
                            details=intercepted.bound,
                        )

            return wrapper

        return decorator

    # ....................... #

    def contextmanager(
        self,
        site: str | None = None,
    ) -> Callable[
        [Callable[P, AbstractContextManager[R]]], Callable[P, AbstractContextManager[R]]
    ]:
        """Wrap a context manager function to intercept exceptions."""

        def decorator(
            fn: Callable[P, AbstractContextManager[R]],
        ) -> Callable[P, AbstractContextManager[R]]:
            @wraps(fn)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> AbstractContextManager[R]:
                intercepted = Intercepted.from_callable(fn, *args, site=site, **kwargs)

                try:
                    cm = fn(*args, **kwargs)

                except BaseException as e:
                    reraise_mapped(
                        self.mapper,
                        e,
                        site=intercepted.site,
                        details=intercepted.bound,
                    )

                return ContextManagerExceptionInterceptor(
                    cm=cm,
                    mapper=self.mapper,
                    site=intercepted.site,
                    details=intercepted.bound,
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
            @wraps(fn)
            def wrapper(
                *args: P.args, **kwargs: P.kwargs
            ) -> AbstractAsyncContextManager[R]:
                intercepted = Intercepted.from_callable(fn, *args, site=site, **kwargs)

                try:
                    cm = fn(*args, **kwargs)

                except BaseException as e:
                    reraise_mapped(
                        self.mapper,
                        e,
                        site=intercepted.site,
                        details=intercepted.bound,
                    )

                return AsyncContextManagerExceptionInterceptor(
                    cm=cm,
                    mapper=self.mapper,
                    site=intercepted.site,
                    details=intercepted.bound,
                )

            return wrapper

        return decorator
