"""Neo4j database/label relation resolution.

For v1 the graph "relation" is just the (static) database name; labels and edge types
come from the :class:`GraphModuleSpec` kinds. This re-exports the shared resolution
helpers so wiring code can resolve a database name per tenant if configured.
"""

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
    "resolve_neo4j_database",
]

# ----------------------- #


async def resolve_neo4j_database(
    spec: NamedResourceSpec,
    tenant_id: UUID | None,
) -> str:
    """Resolve a database :class:`NamedResourceSpec` to a concrete name for *tenant_id*."""

    return await resolve_value(spec, tenant_id)
