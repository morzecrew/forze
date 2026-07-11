"""Yandex Cloud KMS key directory — resolve a tenant to its symmetric key id.

Yandex Cloud mints a key id itself (you may only choose a *name*), and the crypto API
addresses keys by id — so, unlike Vault, AWS aliases, or GCP CryptoKey ids, a tenant's
key id **cannot be derived from the tenant id**. A
:class:`~forze.application.contracts.crypto.TenantTemplateKeyDirectory` therefore does
not work here; this directory looks the id up by name instead.

Names come from a template (``tenant-{tenant_id}``), matching what
:class:`~forze_kms.yc.adapters.YcKmsTenantProvisioner` creates, so the provisioned key
and the encrypt-path key can never drift. The keyring caches the resolved tenant → key-id
mapping, so the lookup runs once per tenant rather than per operation.
"""

from typing import final

import attrs

from forze.application.contracts.crypto import KeyDirectoryPort, KeyRef
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import exc

from ..kernel.client import YcKmsClientPort

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class YcKmsKeyDirectory(KeyDirectoryPort):
    """Resolve a tenant to its Yandex Cloud KMS key by looking the name up in a folder."""

    client: YcKmsClientPort
    """Yandex Cloud KMS client."""

    folder_id: str
    """The folder the tenants' keys live in."""

    template: str = "tenant-{tenant_id}"
    """``str.format``-style template naming a tenant's key, taking ``{tenant_id}``."""

    default_key_id: str | None = None
    """Key id used when no tenant is bound. ``None`` rejects an unbound-tenant encrypt."""

    # ....................... #

    def key_name_for(self, tenant: TenantIdentity) -> str:
        """The key name *tenant* is provisioned under."""

        return self.template.format(tenant_id=tenant.tenant_id)

    # ....................... #

    async def resolve(self, tenant: TenantIdentity | None) -> KeyRef:
        if tenant is None:
            if self.default_key_id is None:
                raise exc.configuration(
                    "Yandex Cloud KMS key directory has no default_key_id, so an "
                    "operation with no bound tenant cannot resolve a key",
                    code="core.crypto.default_key_missing",
                )

            return KeyRef(key_id=self.default_key_id)

        name = self.key_name_for(tenant)
        key_id = await self.client.find_key_id_by_name(self.folder_id, name)

        if key_id is None:
            raise exc.precondition(
                f"No Yandex Cloud KMS key named {name!r} in folder "
                f"{self.folder_id!r} — the tenant is not provisioned",
                code="core.crypto.tenant_key_not_provisioned",
            )

        return KeyRef(key_id=key_id)
