"""Neo4j transaction manager dep factory."""

from forze.application.contracts.transaction import TransactionManagerPort
from forze.application.execution import ExecutionContext

from ....adapters import Neo4jTxManagerAdapter
from ..keys import Neo4jClientDepKey

# ----------------------- #


def neo4j_txmanager(context: ExecutionContext) -> TransactionManagerPort:
    """Build a Neo4j-backed transaction manager for the execution context."""

    client = context.deps.provide(Neo4jClientDepKey)

    return Neo4jTxManagerAdapter(client=client)
