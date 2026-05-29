"""Mongo collection relation resolution."""

from uuid import UUID

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    RelationSpec,
    coerce_named_resource_spec,
    coerce_relation_spec,
    is_static_relation,
    resolve_value,
)

__all__ = [
    "RelationSpec",
    "NamedResourceSpec",
    "coerce_relation_spec",
    "coerce_named_resource_spec",
    "is_static_relation",
    "relations_match",
    "resolve_mongo_collection",
    "resolve_mongo_named_resource",
]


# ....................... #


def relations_match(left: RelationSpec, right: RelationSpec) -> bool:
    """Return whether two relation specs refer to the same mapping."""

    if is_static_relation(left) and is_static_relation(right):
        return left == right

    return left is right


# ....................... #


async def resolve_mongo_collection(
    spec: RelationSpec,
    tenant_id: UUID | None,
) -> tuple[str, str]:
    """Resolve *spec* to ``(database, collection)`` for *tenant_id*."""

    return await resolve_value(spec, tenant_id)


# ....................... #


async def resolve_mongo_named_resource(
    spec: NamedResourceSpec,
    tenant_id: UUID | None,
) -> str:
    """Resolve *spec* to a physical resource name for *tenant_id*."""

    return await resolve_value(spec, tenant_id)
