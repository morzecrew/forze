"""Key directory — resolves a tenant to its key-encryption-key reference.

This is the encryption analog of tenant routing: it maps a tenant to the
:class:`KeyRef` whose key wraps that tenant's data keys. A single-key deployment
returns the same reference for everyone (:class:`StaticKeyDirectory`); a
per-tenant deployment derives a distinct reference per tenant
(:class:`TenantTemplateKeyDirectory`), which is the BYOK shape — each tenant's
data is wrapped under that tenant's own customer-managed key.

The port is async so a directory can fetch a customer-registered key reference
from a store; the shipped implementations resolve synchronously.
"""

from typing import Awaitable, Protocol, final, runtime_checkable

import attrs

from forze.application.contracts.tenancy import TenantIdentity

from .value_objects import KeyRef

# ----------------------- #


class KeyDirectoryPort(Protocol):
    """Resolve a tenant to the key-encryption key that wraps its data keys."""

    def resolve(self, tenant: TenantIdentity | None) -> Awaitable[KeyRef]:
        """Return the :class:`KeyRef` for *tenant* (``None`` = no tenant bound)."""

        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class KeyDirectoryWithPrevious(Protocol):
    """A key directory that can also name a tenant's **previous** key.

    The migration seam for *replacing* a key-encryption key. A keyring refuses an
    envelope whose ``key_id`` is not the one the directory resolves for the tenant
    (the confused-deputy guard), so simply repointing a tenant at a new KEK would
    strand everything already written under the old one — it could not even be read
    back in order to migrate it.

    Naming the previous key opens a **read overlap**: writes go to the current key
    while reads still accept envelopes under the previous one, so a re-encryption
    sweep can move the data across. Drop the previous key once the sweep is done —
    the same two-phase shape a deterministic (searchable) root rotation uses.

    Optional: a directory that does not implement it simply has no overlap, so an
    existing :class:`KeyDirectoryPort` keeps working unchanged.
    """

    def resolve(self, tenant: TenantIdentity | None) -> Awaitable[KeyRef]:
        """Return the current :class:`KeyRef` for *tenant*."""

        ...  # pragma: no cover

    def resolve_previous(
        self,
        tenant: TenantIdentity | None,
    ) -> Awaitable[KeyRef | None]:
        """Return *tenant*'s previous :class:`KeyRef`, or ``None`` when not migrating."""

        ...  # pragma: no cover


# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class StaticKeyDirectory:
    """Resolve every tenant to one fixed key reference (single-key deployments)."""

    key_ref: KeyRef
    """The single key-encryption-key reference used for all tenants."""

    previous_key_ref: KeyRef | None = None
    """The KEK being migrated away from, set only during a migration overlap.

    While set, reads also accept envelopes wrapped under it, so a re-encryption sweep
    can move the data onto :attr:`key_ref`; drop it once the sweep is done."""

    # ....................... #

    async def resolve(self, tenant: TenantIdentity | None) -> KeyRef:
        _ = tenant
        return self.key_ref

    # ....................... #

    async def resolve_previous(self, tenant: TenantIdentity | None) -> KeyRef | None:
        _ = tenant
        return self.previous_key_ref


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TenantTemplateKeyDirectory:
    """Derive a per-tenant key reference from a ``key_id`` template.

    ``template`` is formatted with the tenant id (e.g.
    ``"tenant/{tenant_id}/cmk"``). When no tenant is bound, ``default_key_id`` is
    used. This is a convenience for deployments that name keys by tenant; a
    BYOK directory that looks up a customer-registered reference would implement
    :class:`KeyDirectoryPort` directly.
    """

    template: str
    """``str.format``-style template taking ``{tenant_id}``."""

    default_key_id: str
    """Key id used when no tenant is bound."""

    version: str | None = None
    """Optional fixed key version applied to every resolved reference."""

    previous_template: str | None = None
    """The template being migrated away from, set only during a migration overlap.

    While set, reads also accept envelopes whose key id it resolves, so a re-encryption
    sweep can move every tenant onto :attr:`template`; drop it once the sweep is done.
    This is the whole-deployment shape (a key *naming* change). To replace one tenant's
    key — a BYOK customer supplying a new one — resolve the reference from a store and
    implement :class:`KeyDirectoryWithPrevious` directly."""

    previous_default_key_id: str | None = None
    """Previous key id used when no tenant is bound (pairs with :attr:`previous_template`)."""

    # ....................... #

    async def resolve(self, tenant: TenantIdentity | None) -> KeyRef:
        if tenant is None:
            return KeyRef(key_id=self.default_key_id, version=self.version)

        return KeyRef(
            key_id=self.template.format(tenant_id=tenant.tenant_id),
            version=self.version,
        )

    # ....................... #

    async def resolve_previous(self, tenant: TenantIdentity | None) -> KeyRef | None:
        if tenant is None:
            if self.previous_default_key_id is None:
                return None

            return KeyRef(key_id=self.previous_default_key_id, version=self.version)

        if self.previous_template is None:
            return None

        return KeyRef(
            key_id=self.previous_template.format(tenant_id=tenant.tenant_id),
            version=self.version,
        )
