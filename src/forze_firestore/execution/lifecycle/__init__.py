"""Firestore lifecycle steps (client pool startup and shutdown)."""

from .pool import (
    FirestoreShutdownHook,
    FirestoreStartupHook,
    firestore_lifecycle_step,
    routed_firestore_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "FirestoreShutdownHook",
    "FirestoreStartupHook",
    "firestore_lifecycle_step",
    "routed_firestore_lifecycle_step",
]
