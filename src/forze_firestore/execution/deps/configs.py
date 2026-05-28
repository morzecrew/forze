"""Firestore dependency integration configs (frozen attrs)."""

import attrs

from forze.application.contracts.tenancy import TenantAwareIntegrationConfig

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreReadOnlyDocumentConfig(TenantAwareIntegrationConfig):
    """Read-only document mapping: ``(database_id, collection_id)``."""

    read: tuple[str, str]
    batch_size: int = 200


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreDocumentConfig(FirestoreReadOnlyDocumentConfig):
    """Read-write document mapping with optional history collection."""

    write: tuple[str, str]
    history: tuple[str, str] | None = None
