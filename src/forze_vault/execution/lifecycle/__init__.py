"""Vault lifecycle steps (client pool startup and shutdown)."""

from .pool import (
    VaultShutdownHook,
    VaultStartupHook,
    vault_lifecycle_step,
)

# ----------------------- #

__all__ = [
    "VaultShutdownHook",
    "VaultStartupHook",
    "vault_lifecycle_step",
]
