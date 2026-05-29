"""RabbitMQ queue namespace resolution."""

from uuid import UUID

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    resolve_value,
)

__all__ = [
    "NamedResourceSpec",
    "coerce_named_resource_spec",
    "resolve_rabbitmq_namespace",
]


# ....................... #


async def resolve_rabbitmq_namespace(
    spec: NamedResourceSpec,
    tenant_id: UUID | None,
) -> str:
    """Resolve *spec* to a physical queue namespace for *tenant_id*."""

    return await resolve_value(spec, tenant_id)
