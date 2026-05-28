"""Contracts for transactional execution boundaries."""

from .deps import TransactionDeps, TransactionManagerDepKey, TransactionManagerDepPort
from .ports import (
    AfterCommitPort,
    TransactionHandle,
    TransactionManagerPort,
    TransactionScopeKey,
)

# ----------------------- #

__all__ = [
    "AfterCommitPort",
    "TransactionManagerPort",
    "TransactionScopeKey",
    "TransactionDeps",
    "TransactionManagerDepKey",
    "TransactionManagerDepPort",
    "TransactionHandle",
]
