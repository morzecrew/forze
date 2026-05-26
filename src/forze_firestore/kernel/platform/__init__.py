"""Firestore platform client."""

from .client import FirestoreClient
from .port import FirestoreClientPort

# ----------------------- #

__all__ = [
    "FirestoreClient",
    "FirestoreClientPort",
]
