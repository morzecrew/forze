"""Operation-plan hooks that serialize an operation under a transaction-scoped lock."""

from .plans import OperationLockKey, SerializeOperation

# ----------------------- #

__all__ = [
    "OperationLockKey",
    "SerializeOperation",
]
