"""Mongo transaction manager dep factory."""

from forze.application.contracts.transaction import TransactionManagerPort
from forze.application.execution import ExecutionContext

from ....adapters import MongoTxManagerAdapter
from ..keys import MongoClientDepKey

# ----------------------- #


def mongo_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    """Build a Mongo-backed transaction manager for the execution context."""

    client = context.deps.provide(MongoClientDepKey)

    return MongoTxManagerAdapter(client=client)
