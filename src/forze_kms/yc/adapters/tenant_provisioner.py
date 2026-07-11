"""Yandex Cloud KMS tenant provisioner — create a tenant's key on onboarding.

The encryption analog of the object-storage / DB-schema tenant provisioners: for a
per-tenant BYOK deployment, each tenant's data is wrapped under that tenant's *own*
symmetric key — which has to exist before the first encrypt. This provisioner creates
it on ``provision`` so the higher isolation tiers are operationally real rather than
assuming hand-provisioned keys.

It creates the key under the **same** name :class:`~forze_kms.yc.adapters.YcKmsKeyDirectory`
resolves through, so the provisioned key and the encrypt-path key can never drift.
"""

from typing import final

import attrs

from forze.application.contracts.tenancy import TenantIdentity, TenantProvisionerPort

from ..kernel.client import YcKmsClientPort
from .key_directory import YcKmsKeyDirectory

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class YcKmsTenantProvisioner(TenantProvisionerPort):
    """Ensure a tenant's Yandex Cloud KMS key exists when the tenant is onboarded.

    Pair with the keyring's :class:`YcKmsKeyDirectory`: ``provision`` creates the key the
    directory looks up (idempotent — an existing name is a no-op). Teardown is opt-in via
    ``allow_deletion`` (default off): deleting a tenant's KEK makes every value wrapped
    under it unrecoverable, so it is never done implicitly.
    """

    client: YcKmsClientPort
    """Yandex Cloud KMS client."""

    directory: YcKmsKeyDirectory
    """The keyring's key directory — supplies the folder and the tenant's key name."""

    algorithm: str = "AES_256"
    """Default algorithm for a newly created key (``AES_256`` / ``AES_128``)."""

    description: str | None = None
    """Optional description stamped on a newly created key."""

    allow_deletion: bool = False
    """When ``True``, ``deprovision`` deletes the tenant's key (destructive). Default off."""

    # ....................... #

    async def provision(self, tenant: TenantIdentity) -> None:
        folder_id = self.directory.folder_id
        name = self.directory.key_name_for(tenant)

        if await self.client.find_key_id_by_name(folder_id, name) is not None:
            return None  # already provisioned

        await self.client.create_key(
            folder_id,
            name,
            algorithm=self.algorithm,
            description=self.description,
        )

    # ....................... #

    async def deprovision(self, tenant: TenantIdentity) -> None:
        if not self.allow_deletion:
            return None

        folder_id = self.directory.folder_id
        name = self.directory.key_name_for(tenant)
        key_id = await self.client.find_key_id_by_name(folder_id, name)

        if key_id is None:
            return None  # already gone

        await self.client.delete_key(key_id)
