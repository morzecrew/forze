"""Type aliases for usecase plans."""

from typing import Any, Callable, Final, TypeVar

from ..context import ExecutionContext
from ..middleware import (  # type: ignore[import-not-found]
    Finally,
    Guard,
    Middleware,
    OnFailure,
    SuccessHook,
)
from ..usecase import Usecase

# ----------------------- #

U = TypeVar("U", bound=Usecase[Any, Any])

# ....................... #

GuardFactory = Callable[[ExecutionContext], Guard[Any]]
"""Factory that builds a guard from execution context."""

SuccessHookFactory = Callable[[ExecutionContext], SuccessHook[Any, Any]]
"""Factory that builds a success hook from execution context."""

FinallyFactory = Callable[[ExecutionContext], Finally[Any, Any]]
"""Factory that builds a finally hook from execution context."""

OnFailureFactory = Callable[[ExecutionContext], OnFailure[Any]]
"""Factory that builds an on-failure hook from execution context."""

MiddlewareFactory = Callable[[ExecutionContext], Middleware[Any, Any]]
"""Factory that builds a middleware from execution context."""

WILDCARD: Final[str] = "*"
"""Wildcard operation key for default/fallback plans."""
