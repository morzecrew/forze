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
    routed_firestore_lifecycle_step,
)
from .kernel.platform import (
    FirestoreClient,
    FirestoreClientPort,
    FirestoreRoutingCredentials,
    RoutedFirestoreClient,
)

# ----------------------- #

__all__ = [
    "FirestoreDepsModule",
    "FirestoreClient",
    "FirestoreClientPort",
    "RoutedFirestoreClient",
    "FirestoreRoutingCredentials",
    "FirestoreClientDepKey",
    "firestore_lifecycle_step",
    "routed_firestore_lifecycle_step",
    "FirestoreDocumentConfig",
    "FirestoreReadOnlyDocumentConfig",
]
