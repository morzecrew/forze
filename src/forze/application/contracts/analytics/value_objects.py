"""Value objects returned by analytics ports."""

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.base.primitives import JsonDict

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class IngestSpec:
    """Append-only ingest target for an analytics route.

    Wraps the ingest :class:`~forze.application.contracts.resolution.RelationSpec`
    (``(namespace, table)`` or a per-tenant resolver). Shared across the warehouse
    integrations (Postgres, BigQuery, ClickHouse) so the ingest target is expressed the
    same way everywhere instead of per-backend flat ``ingest_relation`` / ``ingest_table``
    fields.
    """

    relation: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Ingest target ``(namespace, table)`` or ``(tenant_id) -> (namespace, table)`` resolver."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AnalyticsAppendResult:
    """Result of an append-only analytics ingest batch."""

    accepted: int
    """Number of rows accepted by the adapter."""

    rejected: int = 0
    """Number of rows rejected when the engine reports partial failures."""

    errors: tuple[JsonDict, ...] = attrs.field(factory=tuple)
    """Optional row-level errors (capped by the integration client)."""
