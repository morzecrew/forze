"""Contracts for tenant-scoped static or dynamic value resolution."""

from .helpers import resolve_value
from .types import MaybeAwaitable, ValueResolver

# ----------------------- #

__all__ = [
    "MaybeAwaitable",
    "ValueResolver",
    "resolve_value",
]
