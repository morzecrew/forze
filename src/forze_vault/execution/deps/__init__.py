"""Dependency registration for Vault."""

from .keys import VaultClientDepKey
from .module import VaultDepsModule

# ----------------------- #

__all__ = [
    "VaultClientDepKey",
    "VaultDepsModule",
]
