"""Postgres relation resolution for gateways and integration configs."""

from typing import TYPE_CHECKING
from uuid import UUID

from forze.application.contracts.resolution import (
    RelationSpec,
    coerce_relation_spec,
    is_static_relation,
    resolve_value,
)
from forze.application.contracts.resolution import (
    require_static_relation as _require_static_relation_contract,
)

if TYPE_CHECKING:
    from forze_postgres.kernel.gateways.base import PostgresQualifiedName

__all__ = [
    "RelationSpec",
    "coerce_relation_spec",
    "is_static_relation",
    "require_static_relation",
    "resolve_postgres_qname",
]

# ....................... #


def require_static_relation(
    spec: RelationSpec,
    *,
    document_name: str,
    field: str,
) -> tuple[str, str]:
    """Return *spec* when static; fail for dynamic resolvers at schema validation."""

    return _require_static_relation_contract(
        spec,
        route_name=document_name,
        field=field,
        integration="Postgres",
        omit_hint=(
            "Omit postgres_document_schema_validation_lifecycle_step for this route, "
            "or use static relations."
        ),
    )


# ....................... #


async def resolve_postgres_qname(
    spec: RelationSpec,
    tenant_id: UUID | None,
) -> "PostgresQualifiedName":
    """Resolve *spec* to a qualified Postgres name for *tenant_id*."""

    from forze_postgres.kernel.gateways.base import PostgresQualifiedName

    resolved = await resolve_value(spec, tenant_id)
    return PostgresQualifiedName(*resolved)
