"""Contracts for transactional execution boundaries."""

from .deps import (
    TX_LOCK_UNSUPPORTED_CODE,
    TransactionDeps,
    TransactionManagerDepKey,
    TransactionManagerDepPort,
    TxLockDeps,
)
from .ports import (
    COMMIT_AMBIGUOUS_CODE,
    TX_LOCK_OUTSIDE_TRANSACTION_CODE,
    AfterCommitPort,
    IsolationAware,
    IsolationLevel,
    ReadOnlyAware,
    TransactionallyEnlistable,
    TransactionHandle,
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
    TxLockCapable,
    TxLockPort,
    require_tx_lock_in_transaction,
)

# ----------------------- #

__all__ = [
    "COMMIT_AMBIGUOUS_CODE",
    "TX_LOCK_OUTSIDE_TRANSACTION_CODE",
    "TX_LOCK_UNSUPPORTED_CODE",
    "AfterCommitPort",
    "IsolationAware",
    "IsolationLevel",
    "ReadOnlyAware",
    "TransactionManagerPort",
    "TransactionScopeKey",
    "TransactionDeps",
    "TransactionManagerDepKey",
    "TransactionManagerDepPort",
    "TransactionHandle",
    "TransactionallyEnlistable",
    "TxCapabilities",
    "TxLockCapable",
    "TxLockDeps",
    "TxLockPort",
    "require_tx_lock_in_transaction",
]
