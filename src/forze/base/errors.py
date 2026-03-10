"""Core error types and error-handling utilities for the base layer.

Provides a small hierarchy of :class:`CoreError` subclasses used across the
application, together with helpers and decorators for converting arbitrary
exceptions into structured core errors in a consistent way.
"""

import asyncio
import inspect
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from functools import lru_cache, wraps
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    ContextManager,
    Iterator,
    Mapping,
    Optional,
    ParamSpec,
    Protocol,
    TypeGuard,
    TypeVar,
    overload,
)

import attrs
from pydantic import ValidationError as PydanticValidationError

# ----------------------- #


@attrs.define(slots=True, eq=False)
class CoreError(Exception):
    """Base core error for the application.

    All domain- and application-level errors that should be surfaced to
    callers should derive from :class:`CoreError` so they can be handled
    uniformly by infrastructure and presentation layers.
    """

    message: str = "An internal error occurred"
    """Message of the error."""

    code: str = attrs.field(default="internal_error", kw_only=True)
    """Code of the error."""

    details: Optional[Mapping[str, Any]] = attrs.field(default=None, kw_only=True)
    """Optional details of the error."""

    # ....................... #

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"


# ....................... #


@attrs.define(slots=True, eq=False)
class NotFoundError(CoreError):
    """Error raised when a requested resource cannot be found."""

    code: str = attrs.field(default="not_found", kw_only=True)
    message: str = "Resource not found"


# ....................... #


@attrs.define(slots=True, eq=False)
class ConflictError(CoreError):
    """Error raised when an operation encounters a conflicting state."""

    code: str = attrs.field(default="conflict", kw_only=True)
    message: str = "State conflict occured"


# ....................... #


@attrs.define(slots=True, eq=False)
class ValidationError(CoreError):
    """Error raised when validation of user or external input fails."""

    code: str = attrs.field(default="validation_error", kw_only=True)
    message: str = "Validation failed"


# ....................... #


@attrs.define(slots=True, eq=False)
class InfrastructureError(CoreError):
    """Error raised when an infrastructure component (DB, cache, etc.) fails."""

    code: str = "infrastructure_error"
    message: str = "An infrastructure error occurred"


# ....................... #


@attrs.define(slots=True, eq=False)
class ConcurrencyError(CoreError):
    """Error raised when a concurrency conflict occurs."""

    code: str = "concurrency_error"
    message: str = "Concurrency conflict occurred"


# ....................... #


class ErrorHandler(Protocol):
    """Callable protocol for converting exceptions into :class:`CoreError`."""

    def __call__(self, e: Exception, op: str, **kwargs: Any) -> CoreError: ...


def _default_error_hanlder(e: Exception, op: str, **kwargs: Any) -> Optional[CoreError]:
    """Best-effort mapping of low-level exceptions to :class:`CoreError`."""

    err: Optional[CoreError] = None

    match e:
        case PydanticValidationError():
            err = ValidationError(message=e.title)

        case _:
            pass

    return err


def error_handler(fn: ErrorHandler) -> ErrorHandler:
    """Decorator that applies :func:`_default_error_hanlder` before ``fn``."""

    def decorator(fn: ErrorHandler) -> ErrorHandler:
        @wraps(fn)
        def wrapper(e: Exception, op: str, **kwargs: Any) -> CoreError:
            err = _default_error_hanlder(e, op, **kwargs)

            if err is not None:
                return err

            return fn(e, op, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator(fn)


# ....................... #
# Type guards


def _is_awaitable(obj: Any) -> TypeGuard[Awaitable[Any]]:
    """Return ``True`` if *obj* is an awaitable (has ``__await__``)."""

    return hasattr(obj, "__await__")


def _is_contextmanager(obj: Any) -> TypeGuard[ContextManager[Any]]:
    """Return ``True`` if *obj* is a synchronous context-manager function."""

    return (
        inspect.isfunction(obj)
        and obj.__annotations__.get("return") is AbstractContextManager
    )


def _is_async_contextmanager(obj: Any) -> TypeGuard[AsyncContextManager[Any]]:
    """Return ``True`` if *obj* is an async context-manager function."""

    return (
        inspect.isfunction(obj)
        and obj.__annotations__.get("return") is AbstractAsyncContextManager
    )


def _is_async_iterator(obj: Any) -> TypeGuard[AsyncIterator[Any]]:
    """Return ``True`` if *obj* implements the async-iterator protocol."""

    return hasattr(obj, "__aiter__") and hasattr(obj, "__anext__")


def _is_iterator(obj: Any) -> TypeGuard[Iterator[Any]]:
    """Return ``True`` if *obj* implements the iterator protocol.

    Excludes ``str``, ``bytes``, ``bytearray``, and ``memoryview`` which
    are iterable but not considered iterators in this context.
    """

    if isinstance(obj, (str, bytes, bytearray, memoryview)):
        return False

    return hasattr(obj, "__iter__") and hasattr(obj, "__next__")


# ....................... #


P = ParamSpec("P")
R = TypeVar("R")


@attrs.define(slots=True)
class _CmWrapper[R](ContextManager[R]):  # pragma: no cover
    """Wrapper that intercepts exceptions from a sync context manager and converts them via an :class:`ErrorHandler`."""

    cm: ContextManager[R]
    h: ErrorHandler
    op: str
    kwargs: dict[str, Any] = attrs.field(factory=dict)

    # ....................... #

    def __enter__(self) -> R:
        try:
            return self.cm.__enter__()
        except CoreError:
            raise
        except Exception as e:
            raise self.h(e, self.op, **self.kwargs) from e

    # ....................... #

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore
        try:
            return self.cm.__exit__(exc_type, exc, tb)  # type: ignore

        except CoreError:
            raise

        except Exception as e:
            raise self.h(e, self.op, **self.kwargs) from e


# ....................... #


@attrs.define(slots=True)
class _AsyncCmWrapper[R](AsyncContextManager[R]):  # pragma: no cover
    """Wrapper that intercepts exceptions from an async context manager and converts them via an :class:`ErrorHandler`."""

    cm: AsyncContextManager[R]
    h: ErrorHandler
    op: str
    kwargs: dict[str, Any] = attrs.field(factory=dict)

    # ....................... #

    async def __aenter__(self) -> R:
        try:
            return await self.cm.__aenter__()
        except CoreError:
            raise
        except Exception as e:
            raise self.h(e, self.op, **self.kwargs) from e

    # ....................... #

    async def __aexit__(self, exc_type, exc, tb) -> bool:  # type: ignore
        try:
            return await self.cm.__aexit__(exc_type, exc, tb)  # type: ignore

        except CoreError:
            raise

        except Exception as e:
            raise self.h(e, self.op, **self.kwargs) from e


# ....................... #


def _wrap_iterator(  # pragma: no cover
    it: Iterator[R],
    h: ErrorHandler,
    op: str,
    **kwargs: Any,
) -> Iterator[R]:
    """Wrap a sync iterator so exceptions are converted via *h*."""

    try:
        for x in it:
            yield x

    except CoreError:
        raise

    except Exception as e:
        raise h(e, op, **kwargs) from e


# ....................... #


async def _wrap_async_iterator(  # pragma: no cover
    it: AsyncIterator[R],
    h: ErrorHandler,
    op: str,
    **kwargs: Any,
) -> AsyncIterator[R]:
    """Wrap an async iterator so exceptions are converted via *h*."""

    try:
        async for x in it:
            yield x

    except CoreError:
        raise

    except Exception as e:
        raise h(e, op, **kwargs) from e


# ....................... #


@lru_cache(maxsize=256)
def _cached_signature(fn: Callable[..., Any]) -> inspect.Signature:
    """Return a cached :class:`inspect.Signature` for *fn*."""

    return inspect.signature(fn)


def _prepare_fn(  # pragma: no cover
    fn: Callable[P, Any],
    op: Optional[str],
    *args: P.args,
    **kwargs: P.kwargs,
) -> tuple[str, dict[str, Any]]:
    """Resolve the operation name and bound arguments for error context."""

    sig = _cached_signature(fn)
    bound = sig.bind_partial(*args, **kwargs)
    bound.apply_defaults()

    all_args = dict(bound.arguments)
    all_args.pop("self", None)
    all_args.pop("cls", None)

    op = op or fn.__name__

    return op, all_args


# ....................... #


def handled(h: ErrorHandler, op: Optional[str] = None):  # type: ignore[no-untyped-def]
    """Decorator that wraps a callable to convert exceptions via *h*.

    Handles coroutines, sync/async generators, context managers, and plain
    callables. :class:`CoreError` instances are re-raised without conversion.

    :param h: Error handler used to convert non-:class:`CoreError` exceptions.
    :param op: Optional operation name passed to the handler; defaults to the
        callable's ``__name__``.
    """

    @overload  # is_awaitable
    def decorator(fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]: ...

    @overload
    def decorator(
        fn: Callable[P, AsyncContextManager[R]],
    ) -> Callable[P, AsyncContextManager[R]]: ...

    @overload
    def decorator(
        fn: Callable[P, ContextManager[R]],
    ) -> Callable[P, ContextManager[R]]: ...

    @overload
    def decorator(
        fn: Callable[P, AsyncIterator[R]],
    ) -> Callable[P, AsyncIterator[R]]: ...

    @overload
    def decorator(fn: Callable[P, Iterator[R]]) -> Callable[P, Iterator[R]]: ...

    @overload
    def decorator(fn: Callable[P, R]) -> Callable[P, R]: ...

    def decorator(fn: Callable[P, Any]) -> Callable[P, Any]:

        if asyncio.iscoroutinefunction(fn):

            @wraps(fn)
            async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
                operation, _ = _prepare_fn(fn, op, *args, **kwargs)

                try:
                    return await fn(*args, **kwargs)

                except Exception as e:  #! omit kwargs for time being
                    raise h(e, operation) from e

            return async_wrapper

        if inspect.isasyncgenfunction(fn):

            @wraps(fn)
            async def async_gen_wrapper(
                *args: P.args, **kwargs: P.kwargs
            ) -> AsyncIterator[Any]:
                operation, _ = _prepare_fn(fn, op, *args, **kwargs)

                try:
                    it = fn(*args, **kwargs)

                except Exception as e:  # pragma: no cover #! omit kwargs for time being
                    raise h(e, operation) from e

                try:
                    async for x in it:
                        yield x

                except Exception as e:  #! omit kwargs for time being
                    raise h(e, operation) from e

            return async_gen_wrapper

        if inspect.isgeneratorfunction(fn):

            @wraps(fn)
            def gen_wrapper(*args: P.args, **kwargs: P.kwargs) -> Iterator[Any]:
                operation, _ = _prepare_fn(fn, op, *args, **kwargs)

                try:
                    it = fn(*args, **kwargs)

                except Exception as e:  # pragma: no cover #! omit kwargs for time being
                    raise h(e, operation) from e

                try:
                    for x in it:
                        yield x

                except Exception as e:  #! omit kwargs for time being
                    raise h(e, operation) from e

            return gen_wrapper

        @wraps(fn)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            operation, _ = _prepare_fn(fn, op, *args, **kwargs)

            try:
                res = fn(*args, **kwargs)

            except Exception as e:  #! omit kwargs for time being
                raise h(e, operation) from e

            if _is_awaitable(res):

                async def _awaited() -> Any:  # pragma: no cover
                    try:
                        return await res

                    except Exception as e:  #! omit kwargs for time being
                        raise h(e, operation) from e

                return _awaited()  # pragma: no cover

            if _is_async_contextmanager(res):
                return _AsyncCmWrapper(res, h, operation)  # pragma: no cover

            if _is_contextmanager(res):
                return _CmWrapper(res, h, operation)  # pragma: no cover

            if _is_async_iterator(res):
                return _wrap_async_iterator(res, h, operation)

            if _is_iterator(res):
                return _wrap_iterator(res, h, operation)

            return res

        return wrapper

    return decorator
