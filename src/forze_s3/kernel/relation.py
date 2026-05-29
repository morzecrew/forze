"""S3 bucket name resolution."""

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
    "resolve_s3_bucket",
]


# ....................... #


async def resolve_s3_bucket(
    spec: NamedResourceSpec,
    tenant_id: UUID | None,
) -> str:
    """Resolve *spec* to a physical bucket name for *tenant_id*."""

    return await resolve_value(spec, tenant_id)
