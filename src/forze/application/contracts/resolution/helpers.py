"""Helpers for resolving static values or tenant-scoped resolvers."""

from typing import cast
from uuid import UUID

from forze.base.asyncio import maybe_await

from .types import MaybeAwaitable, ValueResolver

# ----------------------- #


async def resolve_value[T](spec: T | ValueResolver[T], tenant_id: UUID | None) -> T:
    """Return *spec* when static, otherwise invoke the resolver and await if needed."""

    if not callable(spec):
        return spec

    res = cast(MaybeAwaitable[T], spec(tenant_id))  # type: ignore[redundant-cast]

    return await maybe_await(res)
