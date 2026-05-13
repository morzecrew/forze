"""Type aliases for usecase plans."""

from enum import StrEnum
from typing import Any, Callable, Final, TypeVar

from ..context import ExecutionContext
from ..middleware import (
    Effect,
    Finally,
    Guard,
    Middleware,
    OnFailure,
)
from ..usecase import Usecase

# ----------------------- #

U = TypeVar("U", bound=Usecase[Any, Any])

# ....................... #

GuardFactory = Callable[[ExecutionContext], Guard[Any]]
"""Factory that builds a guard from execution context."""

EffectFactory = Callable[[ExecutionContext], Effect[Any, Any]]
"""Factory that builds an effect from execution context."""

FinallyFactory = Callable[[ExecutionContext], Finally[Any, Any]]
"""Factory that builds a finally hook from execution context."""

OnFailureFactory = Callable[[ExecutionContext], OnFailure[Any]]
"""Factory that builds an on-failure hook from execution context."""

MiddlewareFactory = Callable[[ExecutionContext], Middleware[Any, Any]]
"""Factory that builds a middleware from execution context."""

OpKey = str | StrEnum
"""Operation identifier (string or enum)."""

WILDCARD: Final[str] = "*"
"""Wildcard operation key for default/fallback plans."""
