"""Helpers for resolving static values or tenant-scoped resolvers."""

from collections.abc import Awaitable, Callable
from typing import cast
from uuid import UUID

from forze.base.asyncio import maybe_await
from forze.base.primitives import OnceCell

from .specs import NamedResourceSpec, is_static_named_resource
from .types import MaybeAwaitable, ValueResolver

# ----------------------- #
# move to tenancy contract?


async def resolve_value[T](spec: T | ValueResolver[T], tenant_id: UUID | None) -> T:
    """Return *spec* when static, otherwise invoke the resolver and await if needed."""

    if not callable(spec):
        return spec

    res = cast(MaybeAwaitable[T], spec(tenant_id))  # type: ignore[redundant-cast]

    return await maybe_await(res)


# ....................... #


async def resolve_scoped_namespace(
    spec: NamedResourceSpec,
    *,
    tenant_id: UUID | None,
    cell: OnceCell[str],
    resolver: Callable[[NamedResourceSpec, UUID | None], Awaitable[str]] = resolve_value,
) -> str:
    """Resolve a per-tenant namespace spec to a name, memoizing only static specs.

    Consolidates the resolve-and-memoize idiom duplicated across queue / storage / search /
    analytics / durable adapters: a static (tenant-independent) name is resolved once and
    cached in *cell*; a dynamic ``(tenant_id) -> str`` resolver is re-resolved on every call
    because the bound tenant varies (the adapter may be shared across tenants). *resolver*
    defaults to the generic :func:`resolve_value`; pass a backend wrapper (e.g.
    ``resolve_temporal_queue``) to keep its naming/validation.
    """

    async def _factory() -> str:
        return await resolver(spec, tenant_id)

    return await cell.resolve(_factory, cache=is_static_named_resource(spec))
