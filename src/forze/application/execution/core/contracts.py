from typing import TYPE_CHECKING, Awaitable, Callable, Protocol

from .value_objects import Outcome

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #

type NextCall[Args, R] = Callable[[Args], Awaitable[R]]
"""Next middleware or operation handler in the chain."""

# ....................... #


class Middleware[Args, R](Protocol):  # pragma: no cover
    """Protocol for middleware that wraps the next call in a chain."""

    def __call__(self, next: NextCall[Args, R], args: Args) -> Awaitable[R]: ...


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

    def __call__(self, args: Args, outcome: Outcome[R]) -> Awaitable[None]: ...


# ....................... #


class Handler[Args, R](Protocol):  # pragma: no cover
    """Protocol for an operation handler that can be executed."""

    def __call__(self, args: Args) -> Awaitable[R]: ...


# ....................... #


class LifecycleHook(Protocol):
    """Protocol for a lifecycle hook that can be executed."""

    def __call__(self, ctx: "ExecutionContext") -> Awaitable[None]: ...
