"""Firestore document gateways."""

from .history import FirestoreHistoryGateway
from .read import FirestoreReadGateway
from .write import FirestoreWriteGateway

__all__ = [
    "FirestoreReadGateway",
    "FirestoreWriteGateway",
    "FirestoreHistoryGateway",
]
