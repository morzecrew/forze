"""Firestore dependency integration configs (frozen attrs)."""

from typing import Literal

import attrs

from forze.application.contracts.resolution import RelationSpec, coerce_relation_spec
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreReadOnlyDocumentConfig(TenantAwareIntegrationConfig):
    """Read-only document mapping: ``(database_id, collection_id)``."""

    read: RelationSpec = attrs.field(converter=coerce_relation_spec)
    batch_size: int = 200

    read_validation: Literal["strict", "trusted"] = "strict"
    """Row decode mode for reads (``trusted`` skips Pydantic validation)."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreDocumentConfig(FirestoreReadOnlyDocumentConfig):  # type: ignore[no-untyped-def]
    """Read-write document mapping with optional history collection."""

    write: RelationSpec = attrs.field(converter=coerce_relation_spec)
    history: RelationSpec | None = attrs.field(  # type: ignore[var-annotated]
        default=None,
        converter=lambda v: coerce_relation_spec(v) if v is not None else None,
    )
