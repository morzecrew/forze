"""Dependency keys for Firestore integration."""

from forze.application.contracts.deps import DepKey

from ...kernel.client import FirestoreClientPort

FirestoreClientDepKey = DepKey[FirestoreClientPort]("firestore_client")
