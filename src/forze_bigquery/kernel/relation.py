"""BigQuery relation resolution for integration configs."""

from uuid import UUID

from forze.application.contracts.resolution import (
    RelationSpec,
    coerce_relation_spec,
    resolve_value,
)

__all__ = [
    "RelationSpec",
    "coerce_relation_spec",
    "resolve_bigquery_ingest_target",
]

# ....................... #


async def resolve_bigquery_ingest_target(
    spec: RelationSpec,
    tenant_id: UUID | None,
) -> tuple[str, str]:
    """Resolve *spec* to ``(dataset, table)`` for *tenant_id*."""

    return await resolve_value(spec, tenant_id)
