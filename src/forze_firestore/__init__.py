"""Firestore integration for Forze."""

from ._compat import require_firestore

require_firestore()

# ....................... #

from .execution import (
    FirestoreClientDepKey,
    FirestoreDepsModule,
    FirestoreDocumentConfig,
    FirestoreReadOnlyDocumentConfig,
    firestore_lifecycle_step,
)
from .kernel.platform import (
    FirestoreClient,
    FirestoreClientPort,
)

# ----------------------- #

__all__ = [
    "FirestoreDepsModule",
    "FirestoreClient",
    "FirestoreClientPort",
    "FirestoreClientDepKey",
    "firestore_lifecycle_step",
    "FirestoreDocumentConfig",
    "FirestoreReadOnlyDocumentConfig",
]
