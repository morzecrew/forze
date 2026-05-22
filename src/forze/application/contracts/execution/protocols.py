from typing import TYPE_CHECKING, Any, Awaitable, Callable, Protocol

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

    from .value_objects import Outcome

# ----------------------- #

type MiddlewareNextCall[Args, R] = Callable[[Args], Awaitable[R]]
"""Next middleware or operation handler in the chain."""

# ....................... #


class Middleware[Args, R](Protocol):  # pragma: no cover
    """Protocol for middleware that wraps the next call in a chain."""

    def __call__(
        self,
        next: MiddlewareNextCall[Args, R],
        args: Args,
    ) -> Awaitable[R]: ...


# ....................... #


class Before[Args](Protocol):  # pragma: no cover
    """Protocol for a hook that runs before the operation handler."""

    def __call__(self, args: Args) -> Awaitable[None]: ...


# ....................... #


class OnSuccess[Args, R](Protocol):  # pragma: no cover
    """Protocol for a hook that runs after the operation handler succeeds."""

    def __call__(self, args: Args, result: R) -> Awaitable[None]: ...


# ....................... #


class OnFailure[Args](Protocol):  # pragma: no cover
    """Protocol for a hook that runs after the operation handler fails."""

    def __call__(self, args: Args, exc: Exception) -> Awaitable[None]: ...


# ....................... #


class Finally[Args, R](Protocol):  # pragma: no cover
    """Protocol for a hook that runs after the operation handler finishes (success or failure)."""

    def __call__(self, args: Args, outcome: "Outcome[R]") -> Awaitable[None]: ...


# ....................... #


class Handler[Args, R](Protocol):  # pragma: no cover
    """Protocol for an operation handler that can be executed."""

    def __call__(self, args: Args) -> Awaitable[R]: ...


# ....................... #


class LifecycleHook(Protocol):
    """Protocol for a lifecycle hook that can be executed."""

    def __call__(self, ctx: "ExecutionContext") -> Awaitable[None]: ...


# ....................... #
# Factories


class MiddlewareFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a middleware."""

    def __call__(self, ctx: "ExecutionContext") -> Middleware[Any, Any]: ...


# ....................... #


class BeforeFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a before hook."""

    def __call__(self, ctx: "ExecutionContext") -> Before[Any]: ...


# ....................... #


class OnSuccessFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a on success hook."""

    def __call__(self, ctx: "ExecutionContext") -> OnSuccess[Any, Any]: ...


# ....................... #


class OnFailureFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a on failure hook."""

    def __call__(self, ctx: "ExecutionContext") -> OnFailure[Any]: ...


# ....................... #


class FinallyFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a finally hook."""

    def __call__(self, ctx: "ExecutionContext") -> Finally[Any, Any]: ...


# ....................... #


class HandlerFactory(Protocol):  # pragma: no cover
    """Protocol for a factory that builds a handler."""

    def __call__(self, ctx: "ExecutionContext") -> Handler[Any, Any]: ...
