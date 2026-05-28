"""Type aliases for tenant-scoped value resolution."""

from collections.abc import Awaitable, Callable
from uuid import UUID

# ----------------------- #

type MaybeAwaitable[T] = T | Awaitable[T]
"""Sync value or awaitable (see :func:`forze.base.asyncio.maybe_await`)."""

type ValueResolver[T] = Callable[[UUID | None], MaybeAwaitable[T]]
"""Resolve a value from optional tenant id (``None`` for tenant-unaware routes)."""
