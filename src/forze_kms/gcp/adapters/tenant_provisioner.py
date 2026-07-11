"""GCP KMS tenant provisioner — create a tenant's per-tenant CryptoKey on onboarding.

The encryption analog of the object-storage / DB-schema tenant provisioners: for a
per-tenant BYOK deployment, each tenant's data is wrapped under that tenant's *own*
CryptoKey — which has to exist before the first encrypt. This provisioner creates it
on ``provision`` so the higher isolation tiers are operationally real rather than
assuming hand-provisioned keys.

It derives the key from the **same** directory the keyring resolves through, so the
provisioned key and the encrypt-path key can never drift. A CryptoKey id is
caller-chosen, so a template directory addresses a tenant's key directly — e.g.
``TenantTemplateKeyDirectory(template="projects/p/locations/global/keyRings/app/cryptoKeys/tenant-{tenant_id}")``.
The **key ring must already exist**; it is a long-lived, shared resource, not a
per-tenant one.
"""

from typing import final

import attrs

from forze.application.contracts.crypto import KeyDirectoryPort
from forze.application.contracts.tenancy import TenantIdentity, TenantProvisionerPort
from forze.base.exceptions import exc

from ..kernel.client import GcpKmsClientPort

# ----------------------- #

_CRYPTO_KEYS = "/cryptoKeys/"


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class GcpKmsTenantProvisioner(TenantProvisionerPort):
    """Ensure a tenant's GCP KMS CryptoKey exists when the tenant is onboarded.

    Pair with the keyring's :class:`KeyDirectoryPort`: ``provision`` resolves the tenant
    to its :class:`KeyRef` — a CryptoKey resource name — and creates that key (idempotent:
    an existing key is a no-op). Teardown is opt-in via ``allow_deletion`` (default off):
    destroying a tenant's KEK makes every value wrapped under it unrecoverable, so it is
    never done implicitly.
    """

    client: GcpKmsClientPort
    """GCP KMS client."""

    directory: KeyDirectoryPort
    """The keyring's key directory — resolves the tenant to the CryptoKey to create."""

    allow_deletion: bool = False
    """When ``True``, ``deprovision`` schedules every version of the tenant's key for
    destruction (destructive). Default off."""

    # ....................... #

    async def _key_name_for(self, tenant: TenantIdentity) -> str:
        key_ref = await self.directory.resolve(tenant)

        if _CRYPTO_KEYS not in key_ref.key_id:
            raise exc.configuration(
                "GCP KMS provisioning needs the key directory to resolve a tenant to a "
                "CryptoKey resource name (…/keyRings/<ring>/cryptoKeys/<key>); got "
                f"{key_ref.key_id!r}",
                code="core.crypto.key_id_not_a_crypto_key",
            )

        return key_ref.key_id

    # ....................... #

    async def provision(self, tenant: TenantIdentity) -> None:
        key_name = await self._key_name_for(tenant)
        parent, _, crypto_key_id = key_name.partition(_CRYPTO_KEYS)

        await self.client.ensure_crypto_key(parent, crypto_key_id)

    # ....................... #

    async def deprovision(self, tenant: TenantIdentity) -> None:
        # Google Cloud KMS cannot delete a CryptoKey — destroying every version is the
        # strongest teardown available, and the (now empty) key resource remains.
        if not self.allow_deletion:
            return None

        key_name = await self._key_name_for(tenant)

        await self.client.destroy_crypto_key_versions(key_name)
