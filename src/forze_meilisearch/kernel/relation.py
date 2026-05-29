"""Meilisearch index UID resolution."""

from uuid import UUID

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
    resolve_value,
)

__all__ = [
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "is_static_named_resource",
    "resolve_meilisearch_index_uid",
]


# ....................... #


async def resolve_meilisearch_index_uid(
    spec: NamedResourceSpec,
    tenant_id: UUID | None,
) -> str:
    """Resolve *spec* to a physical index UID for *tenant_id*."""

    return await resolve_value(spec, tenant_id)
