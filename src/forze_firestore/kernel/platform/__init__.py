"""Firestore platform client."""

from .client import FirestoreClient
from .port import FirestoreClientPort
from .routed_client import RoutedFirestoreClient
from .routing_credentials import FirestoreRoutingCredentials

# ----------------------- #

__all__ = [
    "FirestoreClient",
    "FirestoreClientPort",
    "RoutedFirestoreClient",
    "FirestoreRoutingCredentials",
]
