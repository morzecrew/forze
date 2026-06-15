"""Vault Transit tenant provisioner — create a tenant's per-tenant key on onboarding.

The encryption analog of the object-storage / DB-schema tenant provisioners: for a
per-tenant BYOK deployment (a :class:`~forze.application.contracts.crypto.TenantTemplateKeyDirectory`
or any per-tenant :class:`~forze.application.contracts.crypto.KeyDirectoryPort`), each tenant's
data is wrapped under that tenant's *own* Transit key — which has to exist before the first
encrypt. This provisioner creates it on ``provision`` so the higher tiers are operationally
real rather than assuming hand-provisioned keys.

It derives the key name from the **same** directory the keyring resolves through, so the
provisioned key and the encrypt-path key can never drift. Default ``key_type`` is
``aes256-gcm96`` (data-key generation / envelope encryption); use ``rsa-2048`` /
``ecdsa-p256`` for a per-tenant signing key.
"""

from typing import final

import attrs

from forze.application.contracts.crypto import KeyDirectoryPort
from forze.application.contracts.tenancy import TenantIdentity, TenantProvisionerPort

from ..kernel.client import VaultClientPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class VaultTransitTenantProvisioner(TenantProvisionerPort):
    """Ensure a tenant's Vault Transit key exists when the tenant is onboarded.

    Pair with the keyring's :class:`KeyDirectoryPort`: ``provision`` resolves the tenant to
    its :class:`KeyRef` and creates that Transit key (idempotent — a re-provision is a no-op).
    Teardown is opt-in via ``allow_deletion`` (default off): deleting a tenant's key makes
    every value wrapped under it unrecoverable, so it is never done implicitly.
    """

    client: VaultClientPort
    """Vault client (Transit mount configured on its config)."""

    directory: KeyDirectoryPort
    """The keyring's key directory — resolves the tenant to the key name to create."""

    key_type: str = "aes256-gcm96"
    """Vault Transit key type. ``aes256-gcm96`` (default) for data-key generation /
    envelope encryption; ``rsa-2048`` / ``ecdsa-p256`` for a per-tenant signing key."""

    allow_deletion: bool = False
    """When ``True``, ``deprovision`` deletes the tenant's key (destructive). Default off."""

    # ....................... #

    async def provision(self, tenant: TenantIdentity) -> None:
        key_ref = await self.directory.resolve(tenant)
        await self.client.transit_create_key(key_ref.key_id, key_type=self.key_type)

    # ....................... #

    async def deprovision(self, tenant: TenantIdentity) -> None:
        if not self.allow_deletion:
            return None

        key_ref = await self.directory.resolve(tenant)
        await self.client.transit_delete_key(key_ref.key_id)
