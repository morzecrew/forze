"""Redis namespace resolution."""

from uuid import UUID

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    resolve_value,
)

__all__ = [
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "resolve_redis_namespace",
]


# ....................... #


async def resolve_redis_namespace(
    spec: NamedResourceSpec,
    tenant_id: UUID | None,
) -> str:
    """Resolve *spec* to a Redis key namespace for *tenant_id*."""

    return await resolve_value(spec, tenant_id)
