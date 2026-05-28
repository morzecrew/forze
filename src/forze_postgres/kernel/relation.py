"""Postgres relation resolution for gateways and integration configs."""

from typing import TYPE_CHECKING, TypeAlias, TypeGuard
from uuid import UUID

from forze.application.contracts.resolution import ValueResolver, resolve_value
from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze_postgres.kernel.gateways.base import PostgresQualifiedName

# ----------------------- #

RelationSpec: TypeAlias = tuple[str, str] | ValueResolver[tuple[str, str]]
"""Static ``(schema, relation)`` or tenant-scoped resolver."""


# ....................... #


def coerce_relation_spec(value: object) -> RelationSpec:
    """Normalize config input to :data:`RelationSpec`."""

    if callable(value):
        return value  # type: ignore[return-value]

    if isinstance(value, tuple) and len(value) == 2:  # type: ignore[arg-type]
        schema, name = value  # type: ignore[misc]
        return (str(schema), str(name))  # type: ignore[return-value]

    raise exc.configuration(
        "Relation must be a (schema, name) tuple or a callable resolver",
    )


# ....................... #


def is_static_relation(spec: RelationSpec) -> TypeGuard[tuple[str, str]]:
    return isinstance(spec, tuple)


# ....................... #


def require_static_relation(
    spec: RelationSpec,
    *,
    document_name: str,
    field: str,
) -> tuple[str, str]:
    """Return *spec* when it is a static tuple; fail for dynamic resolvers.

    Startup catalog checks (schema validation, bookkeeping validation) introspect
    fixed ``(schema, relation)`` names and cannot run when relations vary per tenant.
    """

    if is_static_relation(spec):
        return spec

    raise exc.internal(
        f"Document {document_name!r}: {field} uses a dynamic RelationSpec resolver; "
        "startup schema validation requires static (schema, relation) tuples. "
        "Omit postgres_document_schema_validation_lifecycle_step for this route, "
        "or use static relations.",
        code="postgres_dynamic_relation_schema_validation",
        details={"document": document_name, "field": field},
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
