"""Dependency keys for Vault-related services."""

from forze.application.contracts.deps import DepKey

from ...kernel.client import VaultClientPort

# ----------------------- #

VaultClientDepKey: DepKey[VaultClientPort] = DepKey("vault_client")
"""Key used to register a Vault client in the deps container."""
