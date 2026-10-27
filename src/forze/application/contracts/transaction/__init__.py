"""Contracts for transactional execution boundaries."""

from .deps import TransactionDeps, TransactionManagerDepKey, TransactionManagerDepPort
from .ports import (
    COMMIT_AMBIGUOUS_CODE,
    AfterCommitPort,
    IsolationAware,
    IsolationLevel,
    ReadOnlyAware,
    TransactionallyEnlistable,
    TransactionHandle,
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
)

# ----------------------- #

__all__ = [
    "COMMIT_AMBIGUOUS_CODE",
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
]
