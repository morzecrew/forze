"""HashiCorp Vault integration for Forze secrets resolution."""

from forze_vault._compat import require_vault

require_vault()

# ....................... #

from .adapters import VaultKvSecrets, VaultTransitKeyManagement
from .execution import VaultClientDepKey, VaultDepsModule, vault_lifecycle_step
from .kernel.client import VaultClient, VaultClientPort, VaultConfig

# ----------------------- #

__all__ = [
    "VaultClient",
    "VaultClientPort",
    "VaultConfig",
    "VaultClientDepKey",
    "VaultDepsModule",
    "VaultKvSecrets",
    "VaultTransitKeyManagement",
    "vault_lifecycle_step",
]
