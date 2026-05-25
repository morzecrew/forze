"""Dependency keys for Firestore integration."""

from forze.application.contracts.base import DepKey

from ...kernel.platform import FirestoreClientPort

FirestoreClientDepKey = DepKey[FirestoreClientPort]("firestore_client")
