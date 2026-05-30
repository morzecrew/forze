"""Postgres document execution configs."""

from typing import Any, Mapping

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc
from forze_postgres.kernel.relation import RelationSpec, coerce_relation_spec

from ....kernel.gateways import PostgresBookkeepingStrategy

# ----------------------- #


def coerce_read_only_document_config(value: object) -> "PostgresReadOnlyDocumentConfig":
    """Accept :class:`PostgresReadOnlyDocumentConfig` or a legacy mapping."""

    if isinstance(value, PostgresReadOnlyDocumentConfig):
        return value

    if isinstance(value, Mapping):
        return PostgresReadOnlyDocumentConfig(**dict(value))  # type: ignore[arg-type]

    raise exc.configuration(
        "Postgres read-only document config must be PostgresReadOnlyDocumentConfig or a mapping",
    )


# ....................... #


def coerce_document_config(value: object) -> "PostgresDocumentConfig":
    """Accept :class:`PostgresDocumentConfig` or a legacy mapping."""

    if isinstance(value, PostgresDocumentConfig):
        return value

    if isinstance(value, Mapping):
        return PostgresDocumentConfig(**dict(value))  # type: ignore[arg-type]

    raise exc.configuration(
        "Postgres document config must be PostgresDocumentConfig or a mapping",
    )


# ....................... #


def _optional_relation_spec(value: object) -> RelationSpec | None:
    if value is None:
        return None

    return coerce_relation_spec(value)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresReadOnlyDocumentConfig(TenantAwareIntegrationConfig):
    """Configuration for a Postgres read-only document."""

    read: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Read relation (schema, table / view / materialized view) or resolver."""

    nested_field_hints: Mapping[str, Any] | None = None
    """Optional Python types for dot-separated filter/sort paths (see integration docs)."""

    batch_size: int = 200
    """Chunk size for bulk writes and internal chunked offset reads."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresDocumentConfig(PostgresReadOnlyDocumentConfig):  # type: ignore[no-untyped-def]
    """Configuration for a Postgres read-write document."""

    write: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Write relation (schema, table) or resolver."""

    bookkeeping_strategy: PostgresBookkeepingStrategy
    """Bookkeeping strategy."""

    history: RelationSpec | None = attrs.field(
        default=None,
        converter=_optional_relation_spec,
    )
    """History relation (schema, table) or resolver, when history is enabled on the spec."""

    conflict_target: tuple[str, ...] | None = None
    """``ON CONFLICT`` column(s) for ensure/upsert; omitted means infer PK from catalog."""
