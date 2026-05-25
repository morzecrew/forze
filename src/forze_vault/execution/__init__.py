"""Execution wiring for Vault integration."""

from .deps import VaultClientDepKey, VaultDepsModule
from .lifecycle import vault_lifecycle_step

# ----------------------- #

__all__ = [
    "VaultClientDepKey",
    "VaultDepsModule",
    "vault_lifecycle_step",
]
