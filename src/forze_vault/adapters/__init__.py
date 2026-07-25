"""Vault adapters implementing application contracts."""

from .dynamic_secrets import VaultDynamicSecrets
from .jwt_signer import VaultTransitSigner
from .key_management import VaultTransitKeyManagement
from .secrets import VaultKvSecrets
from .tenant_provisioner import VaultTransitTenantProvisioner

# ----------------------- #

__all__ = [
    "VaultDynamicSecrets",
    "VaultKvSecrets",
    "VaultTransitKeyManagement",
    "VaultTransitSigner",
    "VaultTransitTenantProvisioner",
]
