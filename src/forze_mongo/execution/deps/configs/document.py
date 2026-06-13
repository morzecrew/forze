"""Mongo document execution configs."""

from typing import Literal

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

    read_validation: Literal["strict", "trusted"] = "strict"
    """Row decode mode for reads (``trusted`` skips Pydantic validation)."""

    computed_null_ordering: bool = False
    """Honor an explicit per-key ``NULLS FIRST``/``LAST`` that differs from Mongo's native
    null-as-smallest order, by sorting offset reads through an aggregation pipeline (a
    computed null-rank key) instead of a plain ``find().sort()``.

    Off by default — an explicit non-native null ordering is otherwise rejected with a
    clean ``query_feature_unsupported`` error. **Cost:** the computed sort key cannot use
    an index, so Mongo performs an in-memory sort (bounded by its sort-memory limit);
    enable it only for read models where you accept that. The canonical default null
    ordering always uses the native indexed sort regardless of this flag."""


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
