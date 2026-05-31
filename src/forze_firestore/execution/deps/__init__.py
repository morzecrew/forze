"""Firestore dependency keys, module, and factory functions."""

from .configs import FirestoreDocumentConfig, FirestoreReadOnlyDocumentConfig
from .factories import (
    ConfigurableFirestoreDocument,
    ConfigurableFirestoreReadOnlyDocument,
    firestore_txmanager,
)
from .keys import FirestoreClientDepKey
from .module import FirestoreDepsModule

# ----------------------- #

__all__ = [
    "FirestoreDepsModule",
    "FirestoreClientDepKey",
    "FirestoreDocumentConfig",
    "FirestoreReadOnlyDocumentConfig",
    "ConfigurableFirestoreDocument",
    "ConfigurableFirestoreReadOnlyDocument",
    "firestore_txmanager",
]
