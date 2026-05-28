"""Postgres transaction manager dep factory."""

from typing import TYPE_CHECKING

from ....adapters import PostgresTxManagerAdapter
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.transaction import TransactionManagerPort
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def postgres_txmanager(context: "ExecutionContext") -> "TransactionManagerPort":
    """Build a Postgres-backed transaction manager for the execution context.

    :param context: Execution context for resolving the Postgres client.
    :returns: Tx manager port backed by :class:`PostgresTxManagerAdapter`.
    """

    client = context.deps.provide(PostgresClientDepKey)

    return PostgresTxManagerAdapter(client=client)
