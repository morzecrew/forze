"""Firestore transaction manager dep factory."""

from forze.application.contracts.transaction import TransactionManagerPort
from forze.application.execution import ExecutionContext

from ....adapters import FirestoreTxManagerAdapter
from ..keys import FirestoreClientDepKey

# ----------------------- #


def firestore_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    """Build a Firestore-backed transaction manager for the execution context."""

    client = context.deps.provide(FirestoreClientDepKey)

    return FirestoreTxManagerAdapter(client=client)
