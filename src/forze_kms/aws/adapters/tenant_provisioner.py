"""AWS KMS tenant provisioner — create a tenant's per-tenant CMK on onboarding.

The encryption analog of the object-storage / DB-schema tenant provisioners: for a
per-tenant BYOK deployment, each tenant's data is wrapped under that tenant's *own*
CMK — which has to exist before the first encrypt. This provisioner creates it on
``provision`` so the higher isolation tiers are operationally real rather than
assuming hand-provisioned keys.

It derives the key from the **same** directory the keyring resolves through, so the
provisioned key and the encrypt-path key can never drift.

KMS mints a CMK id itself, so the only caller-chosen name a directory can address a
tenant's key by is an **alias**: the directory must resolve a tenant to
``alias/<something>`` (e.g. ``TenantTemplateKeyDirectory(template="alias/tenant-{tenant_id}")``),
and ``provision`` creates the CMK and points that alias at it.
"""

from typing import final

import attrs

from forze.application.contracts.crypto import KeyDirectoryPort
from forze.application.contracts.tenancy import TenantIdentity, TenantProvisionerPort
from forze.base.exceptions import exc

from ..kernel.client import AwsKmsClientPort

# ----------------------- #

_ALIAS_PREFIX = "alias/"

_MIN_PENDING_WINDOW_DAYS = 7
_MAX_PENDING_WINDOW_DAYS = 30


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class AwsKmsTenantProvisioner(TenantProvisionerPort):
    """Ensure a tenant's AWS KMS key exists when the tenant is onboarded.

    Pair with the keyring's :class:`KeyDirectoryPort`: ``provision`` resolves the tenant
    to its :class:`KeyRef` — which must be an ``alias/…`` — and creates the CMK behind
    that alias (idempotent: an existing alias is a no-op). Teardown is opt-in via
    ``allow_deletion`` (default off): destroying a tenant's KEK makes every value wrapped
    under it unrecoverable, so it is never done implicitly.
    """

    client: AwsKmsClientPort
    """AWS KMS client."""

    directory: KeyDirectoryPort
    """The keyring's key directory — resolves the tenant to the alias to create."""

    description: str | None = None
    """Optional description stamped on a newly created CMK."""

    allow_deletion: bool = False
    """When ``True``, ``deprovision`` deletes the alias and schedules the CMK for
    deletion (destructive). Default off."""

    pending_window_days: int = _MAX_PENDING_WINDOW_DAYS
    """Waiting period before a scheduled CMK deletion takes effect (KMS allows 7-30 days).
    KMS never deletes a key immediately; the window is the last chance to cancel."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not (
            _MIN_PENDING_WINDOW_DAYS
            <= self.pending_window_days
            <= _MAX_PENDING_WINDOW_DAYS
        ):
            raise exc.configuration(
                f"AWS KMS pending deletion window must be between "
                f"{_MIN_PENDING_WINDOW_DAYS} and {_MAX_PENDING_WINDOW_DAYS} days",
                code="core.crypto.pending_window_invalid",
            )

    # ....................... #

    async def _alias_for(self, tenant: TenantIdentity) -> str:
        key_ref = await self.directory.resolve(tenant)

        if not key_ref.key_id.startswith(_ALIAS_PREFIX):
            raise exc.configuration(
                "AWS KMS provisioning needs the key directory to resolve a tenant to an "
                f"{_ALIAS_PREFIX!r} name (a CMK id is minted by KMS and cannot be chosen); "
                f"got {key_ref.key_id!r}",
                code="core.crypto.key_id_not_an_alias",
            )

        return key_ref.key_id

    # ....................... #

    async def provision(self, tenant: TenantIdentity) -> None:
        alias = await self._alias_for(tenant)

        if await self.client.find_key_id_by_alias(alias) is not None:
            return None  # already provisioned

        await self.client.create_key_with_alias(alias, description=self.description)

    # ....................... #

    async def deprovision(self, tenant: TenantIdentity) -> None:
        if not self.allow_deletion:
            return None

        alias = await self._alias_for(tenant)
        key_id = await self.client.find_key_id_by_alias(alias)

        if key_id is None:
            return None  # already gone

        # Drop the alias first so the tenant can be re-provisioned while the CMK
        # serves out its deletion window.
        await self.client.delete_alias(alias)
        await self.client.schedule_key_deletion(
            key_id, pending_window_days=self.pending_window_days
        )
