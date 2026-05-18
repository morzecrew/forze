"""Type aliases for usecase plans."""

from typing import Any, Callable, Final

from ..context import ExecutionContext
from ..middlewares import Finally, Guard, Middleware, OnFailure, OnSuccess

# Backward-compatible alias used by registry DAG typing and dispatch hooks.
SuccessHook = OnSuccess

GuardFactory = Callable[[ExecutionContext], Guard[Any]]
"""Factory that builds a guard from execution context."""

SuccessHookFactory = Callable[[ExecutionContext], OnSuccess[Any, Any]]
"""Factory that builds a success hook from execution context."""

FinallyFactory = Callable[[ExecutionContext], Finally[Any, Any]]
"""Factory that builds a finally hook from execution context."""

OnFailureFactory = Callable[[ExecutionContext], OnFailure[Any]]
"""Factory that builds an on-failure hook from execution context."""

MiddlewareFactory = Callable[[ExecutionContext], Middleware[Any, Any]]
"""Factory that builds a middleware from execution context."""

WILDCARD: Final[str] = "*"
"""Wildcard operation key for default/fallback plans."""
