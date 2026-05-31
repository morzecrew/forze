"""Mongo document execution configs."""

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoReadOnlyDocumentConfig(TenantAwareIntegrationConfig):
    """Configuration for a Mongo read-only document."""

    read: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Read collection (database, collection / view)."""

    batch_size: int = 200
    """Chunk size for bulk writes and internal chunked offset reads."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoDocumentConfig(MongoReadOnlyDocumentConfig):  # type: ignore[no-untyped-def]
    """Configuration for a Mongo read-write document."""

    write: RelationSpec = attrs.field(converter=coerce_relation_spec)
    """Write collection (database, collection)."""

    history: RelationSpec | None = attrs.field(  # type: ignore[var-annotated]
        default=None,
        converter=lambda v: coerce_relation_spec(v) if v is not None else None,
    )
    """History collection (database, collection), optional."""
