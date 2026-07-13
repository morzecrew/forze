"""Type aliases for tenant-scoped value resolution."""

from collections.abc import Awaitable, Callable
from uuid import UUID

# ----------------------- #
# move to tenancy contract?

type MaybeAwaitable[T] = T | Awaitable[T]
"""Sync value or awaitable (see :func:`forze.base.asyncio.maybe_await`)."""

# maybe actually rename this into TenantAwareResolver or so
type ValueResolver[T] = Callable[[UUID | None], MaybeAwaitable[T]]
"""Resolve a value from optional tenant id (``None`` for tenant-unaware routes)."""
