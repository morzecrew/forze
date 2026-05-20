from typing import TYPE_CHECKING, Any, Callable

from .contracts import Before, Finally, Handler, Middleware, OnFailure, OnSuccess

if TYPE_CHECKING:
    from ..context import ExecutionContext

# ----------------------- #

type MiddlewareFactory = Callable[["ExecutionContext"], Middleware[Any, Any]]
"""Factory that builds a middleware from execution context."""

type BeforeFactory = Callable[["ExecutionContext"], Before[Any]]
"""Factory that builds a before hook from execution context."""

type OnSuccessFactory = Callable[["ExecutionContext"], OnSuccess[Any, Any]]
"""Factory that builds a on success hook from execution context."""

type OnFailureFactory = Callable[["ExecutionContext"], OnFailure[Any]]
"""Factory that builds a on failure hook from execution context."""

type FinallyFactory = Callable[["ExecutionContext"], Finally[Any, Any]]
"""Factory that builds a finally hook from execution context."""

type HandlerFactory = Callable[["ExecutionContext"], Handler[Any, Any]]
"""Factory that builds an operation from execution context."""
