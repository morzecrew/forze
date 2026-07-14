"""Value objects returned by analytics ports."""

from enum import StrEnum

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.base.primitives import JsonDict

# ----------------------- #


class AnalyticsProvenance(StrEnum):
    """Where an analytics table's rows come from — and therefore whether they can be carried.

    The framework cannot work this out for itself, and the two cases are not close. An
    application that *projects* into its warehouse from documents it already owns can throw the
    warehouse away and recompute it. One that **ingests events straight into it** — the shape a
    ClickHouse or BigQuery pipeline usually takes — has made that warehouse a system of record,
    and there is nothing to recompute it from. From the outside the two look identical: the same
    spec, the same ports, the same rows.

    Guessing is not an option in either direction. Assume *projected* and a portable export
    silently drops the only copy of the data; assume *system of record* and it refuses to export
    a table that was never anything but a cache. So the author declares it.
    """

    UNDECLARED = "undeclared"
    """Nobody has said. Legal at runtime — this is the default, and no existing application is
    affected by it — but a portable export **refuses** rather than guess. "We did not think
    about it" and "there is nothing here to carry" must not look the same."""

    PROJECTED = "projected"
    """Derived from an exportable plane. Not carried in an artifact; the application recomputes
    it on the target. (There is no generic warehouse rebuild — the projection logic is the
    app's, so the recompute is too.)"""

    SYSTEM_OF_RECORD = "system_of_record"
    """The warehouse *is* the source of truth for these rows — nothing else holds them. A
    portable export refuses, loudly: ``AnalyticsQueryPort`` exposes only the app's named
    queries, with no generic full-scan read, so the framework has no way to carry the data and
    no way to rebuild it either."""


# ....................... #


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


def coerce_optional_ingest(value: object) -> "IngestSpec | None":
    """Coerce a config ``ingest=`` value to an :class:`IngestSpec`.

    Passes ``None`` and existing :class:`IngestSpec` through; wraps a raw relation spec
    (``(namespace, table)`` tuple or resolver) so ``ingest=("public", "events")`` works.
    """

    if value is None or isinstance(value, IngestSpec):
        return value

    return IngestSpec(value)  # type: ignore[arg-type]


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
