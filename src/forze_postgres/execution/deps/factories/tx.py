"""Postgres transaction manager and transaction-scoped lock dep factories."""

from typing import TYPE_CHECKING

from ....adapters import PostgresTxLockAdapter, PostgresTxManagerAdapter
from ..keys import PostgresClientDepKey

if TYPE_CHECKING:
    from forze.application.contracts.transaction import TransactionManagerPort, TxLockPort
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def postgres_txmanager(context: "ExecutionContext") -> "TransactionManagerPort":
    """Build a Postgres-backed transaction manager for the execution context.

    :param context: Execution context for resolving the Postgres client.
    :returns: Tx manager port backed by :class:`PostgresTxManagerAdapter`.
    """

    client = context.deps.provide(PostgresClientDepKey)

    return PostgresTxManagerAdapter(client=client)


# ....................... #


def postgres_tx_lock(context: "ExecutionContext") -> "TxLockPort":
    """Build the transaction-scoped lock port of the client :func:`postgres_txmanager` uses.

    :param context: Execution context for resolving the Postgres client.
    :returns: Lock port backed by :class:`PostgresTxLockAdapter`.
    """

    return PostgresTxLockAdapter(client=context.deps.provide(PostgresClientDepKey))
