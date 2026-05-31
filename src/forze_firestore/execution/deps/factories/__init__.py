"""Firestore dependency factories."""

from .document import (
    ConfigurableFirestoreDocument,
    ConfigurableFirestoreReadOnlyDocument,
)
from .tx import firestore_txmanager

# ----------------------- #

__all__ = [
    "ConfigurableFirestoreDocument",
    "ConfigurableFirestoreReadOnlyDocument",
    "firestore_txmanager",
]
