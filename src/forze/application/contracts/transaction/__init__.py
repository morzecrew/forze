"""Contracts for transactional execution boundaries."""

from .deps import TransactionDeps, TransactionManagerDepKey, TransactionManagerDepPort
from .ports import (
    AfterCommitPort,
    IsolationAware,
    IsolationLevel,
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
    "TxCapabilities",
]
