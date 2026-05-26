from contextlib import AbstractAsyncContextManager, AbstractContextManager
from types import TracebackType
from typing import Any, Callable, Mapping, Self, Type

import attrs

from ._utils import cached_signature, reraise_mapped, resolve_site
from .protocols import ExceptionMapper

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class Intercepted:
    """Interception context."""

    site: str
    """The site of the interception."""

    bound: Mapping[str, Any] | None = None
    """The bound context of the interception."""

    # ....................... #

    @classmethod
    def from_callable(
        cls,
        fn: Callable[..., Any],
        *args: Any,
        site: str | None = None,
        **kwargs: Any,
    ) -> Self:
        """Build an :class:`Intercepted` from a callable and its arguments."""

        from forze.base.scrubbing import dump_bound_args_for_errors

        sig = cached_signature(fn)
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()

        all_args = dict(bound.arguments)

        all_args.pop("self", None)
        all_args.pop("cls", None)

        return cls(
            site=resolve_site(fn, site),
            bound=dump_bound_args_for_errors(all_args),
        )


# ....................... #


@attrs.define(slots=True)
class ContextManagerExceptionInterceptor[R](AbstractContextManager[R]):
    """Wrapper that intercepts exceptions from a sync context manager and converts them via an :class:`ExceptionMapper`."""

    cm: AbstractContextManager[R]
    mapper: ExceptionMapper
    site: str
    details: Mapping[str, Any] | None = None

    # ....................... #

    def __enter__(self) -> R:
        try:
            return self.cm.__enter__()

        except BaseException as e:
            reraise_mapped(self.mapper, e, site=self.site, details=self.details)

    # ....................... #

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        try:
            return self.cm.__exit__(exc_type, exc, tb)

        except BaseException as e:
            reraise_mapped(self.mapper, e, site=self.site, details=self.details)


# ....................... #


@attrs.define(slots=True)
class AsyncContextManagerExceptionInterceptor[R](AbstractAsyncContextManager[R]):
    """Wrapper that intercepts exceptions from an async context manager and converts them via an :class:`ExceptionMapper`."""

    cm: AbstractAsyncContextManager[R]
    mapper: ExceptionMapper
    site: str
    details: Mapping[str, Any] | None = None

    # ....................... #

    async def __aenter__(self) -> R:
        try:
            return await self.cm.__aenter__()

        except BaseException as e:
            reraise_mapped(self.mapper, e, site=self.site, details=self.details)

    # ....................... #

    async def __aexit__(
        self,
        exc_type: Type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        try:
            return await self.cm.__aexit__(exc_type, exc, tb)

        except BaseException as e:
            reraise_mapped(self.mapper, e, site=self.site, details=self.details)
