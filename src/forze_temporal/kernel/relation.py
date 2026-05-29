"""Temporal task queue resolution."""

from uuid import UUID

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    resolve_value,
)

__all__ = [
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "resolve_temporal_queue",
]


# ....................... #


async def resolve_temporal_queue(
    spec: NamedResourceSpec,
    tenant_id: UUID | None,
) -> str:
    """Resolve *spec* to a Temporal task queue name for *tenant_id*."""

    return await resolve_value(spec, tenant_id)
