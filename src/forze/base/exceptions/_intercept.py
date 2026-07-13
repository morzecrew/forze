from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from types import TracebackType
from typing import Any, Never

import attrs

from ._utils import (
    BYPASS_INTERCEPTION,
    cached_signature,
    reraise_mapped,
)
from .model import CoreException
from .protocols import ExceptionMapper

# ----------------------- #

DetailsFactory = Callable[[], Mapping[str, Any] | None]
"""Lazy producer of error-context details, invoked only on the error path."""

_PASSTHROUGH: tuple[type[BaseException], ...] = (CoreException, *BYPASS_INTERCEPTION)
"""Exceptions reraised as-is without materializing error details."""

# ....................... #


def materialize_bound_details(
    fn: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind and sanitize call arguments for error-context details.

    Invoked lazily on the error path only — the success path pays nothing for
    signature binding or argument sanitization. As a consequence, the details
    reflect argument state at *failure* time rather than call time: arguments
    mutated by the callee before raising may differ from what was passed in.
    """

    from forze.base.scrubbing import dump_bound_args_for_errors

    sig = cached_signature(fn)
    bound = sig.bind_partial(*args, **kwargs)
    bound.apply_defaults()

    all_args = dict(bound.arguments)

    all_args.pop("self", None)
    all_args.pop("cls", None)

    return dump_bound_args_for_errors(all_args)


# ....................... #


@attrs.define(slots=True)
class ContextManagerExceptionInterceptor[R](AbstractContextManager[R]):
    """Wrapper that intercepts exceptions from a sync context manager and converts them via an :class:`ExceptionMapper`.

    ``details_factory`` is only invoked when a non-passthrough exception is
    intercepted, so successful enter/exit pays no detail-materialization cost.
    """

    cm: AbstractContextManager[R]
    mapper: ExceptionMapper
    site: str
    details_factory: DetailsFactory | None = None

    # ....................... #

    def _reraise(self, e: BaseException) -> Never:
        if isinstance(e, _PASSTHROUGH):
            raise e

        details = None

        if self.details_factory is not None:
            try:
                details = self.details_factory()

            except Exception:  # never mask the original error
                details = None

        reraise_mapped(self.mapper, e, site=self.site, details=details)

    # ....................... #

    def __enter__(self) -> R:
        try:
            return self.cm.__enter__()

        except BaseException as e:
            self._reraise(e)

    # ....................... #

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        try:
            return self.cm.__exit__(exc_type, exc, tb)

        except BaseException as e:
            self._reraise(e)


# ....................... #


@attrs.define(slots=True)
class AsyncContextManagerExceptionInterceptor[R](AbstractAsyncContextManager[R]):
    """Wrapper that intercepts exceptions from an async context manager and converts them via an :class:`ExceptionMapper`.

    ``details_factory`` is only invoked when a non-passthrough exception is
    intercepted, so successful enter/exit pays no detail-materialization cost.
    """

    cm: AbstractAsyncContextManager[R]
    mapper: ExceptionMapper
    site: str
    details_factory: DetailsFactory | None = None

    # ....................... #

    def _reraise(self, e: BaseException) -> Never:
        if isinstance(e, _PASSTHROUGH):
            raise e

        details = None

        if self.details_factory is not None:
            try:
                details = self.details_factory()

            except Exception:  # never mask the original error
                details = None

        reraise_mapped(self.mapper, e, site=self.site, details=details)

    # ....................... #

    async def __aenter__(self) -> R:
        try:
            return await self.cm.__aenter__()

        except BaseException as e:
            self._reraise(e)

    # ....................... #

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool | None:
        try:
            return await self.cm.__aexit__(exc_type, exc, tb)

        except BaseException as e:
            self._reraise(e)
