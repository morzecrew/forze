"""HashiCorp Vault integration for Forze secrets resolution."""

from forze_vault._compat import require_vault

require_vault()

# ....................... #

from .adapters import (
    VaultDynamicSecrets,
    VaultKvSecrets,
    VaultTransitKeyManagement,
    VaultTransitSigner,
    VaultTransitTenantProvisioner,
)
from .execution import VaultClientDepKey, VaultDepsModule, vault_lifecycle_step
from .kernel.client import VaultClient, VaultClientPort, VaultConfig
from .settings import VaultSettings

# ----------------------- #

__all__ = [
    "VaultClient",
    "VaultClientPort",
    "VaultConfig",
    "VaultSettings",
    "VaultClientDepKey",
    "VaultDepsModule",
    "VaultDynamicSecrets",
    "VaultKvSecrets",
    "VaultTransitKeyManagement",
    "VaultTransitSigner",
    "VaultTransitTenantProvisioner",
    "vault_lifecycle_step",
]
