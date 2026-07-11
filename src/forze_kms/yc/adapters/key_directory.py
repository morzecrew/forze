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

    previous_template: str | None = None
    """The key-name template being migrated away from, set only during a migration
    overlap. While set, reads also accept envelopes wrapped under the key it names, so a
    re-encryption sweep can move the data onto :attr:`template`; drop it once done."""

    _previous_ids: dict[str, str] = attrs.field(factory=dict, init=False, repr=False)
    """tenant id → the previous key's minted id, memoized so a migration sweep does not
    re-List per envelope (see :meth:`resolve_previous`)."""

    # ....................... #

    def key_name_for(self, tenant: TenantIdentity) -> str:
        """The key name *tenant* is provisioned under."""

        return self.template.format(tenant_id=tenant.tenant_id)

    # ....................... #

    async def resolve_previous(self, tenant: TenantIdentity | None) -> KeyRef | None:
        """Look up the tenant's *previous* key, or ``None`` when not migrating.

        The keyring asks this once per envelope it cannot match to the tenant's current
        key — that is, for every value a migration sweep reads — and the lookup is an API
        call, so a *found* id is memoized here. This is the safe place for that memo:
        :attr:`previous_template` is frozen, so dropping the overlap means building a new
        directory, which starts with an empty one. An *absent* previous key is never
        memoized, so opening an overlap later is picked up at once.
        """

        if tenant is None or self.previous_template is None:
            return None

        cache_key = str(tenant.tenant_id)
        cached = self._previous_ids.get(cache_key)

        if cached is not None:
            return KeyRef(key_id=cached)

        name = self.previous_template.format(tenant_id=tenant.tenant_id)
        key_id = await self.client.find_key_id_by_name(self.folder_id, name)

        if key_id is None:
            return None

        self._previous_ids[cache_key] = key_id

        return KeyRef(key_id=key_id)

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
