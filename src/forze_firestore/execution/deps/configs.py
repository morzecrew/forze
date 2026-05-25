"""Typed configuration for Firestore document routes."""

from typing import NotRequired, TypedDict


class _BaseFirestoreConfig(TypedDict):
    tenant_aware: NotRequired[bool]


class FirestoreReadOnlyDocumentConfig(_BaseFirestoreConfig):
    """Read-only document mapping: ``(database_id, collection_id)``."""

    read: tuple[str, str]
    batch_size: NotRequired[int]


class FirestoreDocumentConfig(FirestoreReadOnlyDocumentConfig):
    """Read-write document mapping with optional history collection."""

    write: tuple[str, str]
    history: NotRequired[tuple[str, str]]
