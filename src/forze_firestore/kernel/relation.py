"""Firestore collection relation resolution."""

from uuid import UUID

from forze.application.contracts.resolution import (
    RelationSpec,
    coerce_relation_spec,
    is_static_relation,
    resolve_value,
)

__all__ = [
    "RelationSpec",
    "coerce_relation_spec",
    "is_static_relation",
    "relations_match",
    "resolve_firestore_collection",
]


# ....................... #


def relations_match(left: RelationSpec, right: RelationSpec) -> bool:
    """Return whether two relation specs refer to the same mapping."""

    if is_static_relation(left) and is_static_relation(right):
        return left == right

    return left is right


# ....................... #


async def resolve_firestore_collection(
    spec: RelationSpec,
    tenant_id: UUID | None,
) -> tuple[str, str]:
    """Resolve *spec* to ``(database, collection)`` for *tenant_id*."""

    return await resolve_value(spec, tenant_id)
