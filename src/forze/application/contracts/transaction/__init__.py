"""Contracts for transactional execution boundaries."""

from .deps import TransactionDeps, TransactionManagerDepKey, TransactionManagerDepPort
from .ports import (
    AfterCommitPort,
    IsolationAware,
    IsolationLevel,
    TransactionallyEnlistable,
    TransactionHandle,
    TransactionManagerPort,
    TransactionScopeKey,
    TxCapabilities,
)

# ----------------------- #

__all__ = [
    "AfterCommitPort",
    "IsolationAware",
    "IsolationLevel",
    "TransactionManagerPort",
    "TransactionScopeKey",
    "TransactionDeps",
    "TransactionManagerDepKey",
    "TransactionManagerDepPort",
    "TransactionHandle",
    "TransactionallyEnlistable",
    "TxCapabilities",
]
