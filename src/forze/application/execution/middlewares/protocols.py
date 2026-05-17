"""Middleware protocols for usecase chains."""

from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol, runtime_checkable

from .value_objects import Failure, Skip, Success

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #

type NextCall[Args, R] = Callable[[Args], Awaitable[R]]
"""Next middleware or usecase in the chain."""

# ....................... #


@runtime_checkable
class Middleware[Args, R](Protocol):  # pragma: no cover
    """Protocol for middleware that wraps the next call in a chain."""

    async def __call__(self, next: NextCall[Args, R], args: Args) -> R: ...


# ....................... #


@runtime_checkable
class Guard[Args](Protocol):  # pragma: no cover
    """Protocol for a hook that runs before the usecase."""

    async def __call__(self, args: Args) -> None | Skip: ...


# ....................... #


@runtime_checkable
class OnSuccess[Args, R](Protocol):  # pragma: no cover
    """Protocol for a read-only hook that runs after a successful inner call."""

    async def __call__(self, args: Args, result: R) -> None | Skip: ...


# ....................... #


@runtime_checkable
class OnFailure[Args](Protocol):  # pragma: no cover
    """Hook invoked when an :class:`Exception` escapes the inner chain."""

    async def __call__(self, args: Args, exc: Exception) -> None: ...


# ....................... #


@runtime_checkable
class Finally[Args, R](Protocol):  # pragma: no cover
    """Hook invoked after the inner chain finishes (success or failure)."""

    async def __call__(
        self,
        args: Args,
        outcome: Success[R] | Failure,  # noqa: F841
    ) -> None: ...


# ....................... #
# Factories

type GuardFactory = Callable[[ExecutionContext], Guard[Any]]
"""Factory that builds a guard from execution context."""

type OnSuccessFactory = Callable[[ExecutionContext], OnSuccess[Any, Any]]
"""Factory that builds a success hook from execution context."""

type OnFailureFactory = Callable[[ExecutionContext], OnFailure[Any]]
"""Factory that builds an on-failure hook from execution context."""

type FinallyFactory = Callable[[ExecutionContext], Finally[Any, Any]]
"""Factory that builds a finally hook from execution context."""

type MiddlewareFactory = Callable[[ExecutionContext], Middleware[Any, Any]]
"""Factory that builds a middleware from execution context."""
