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

from typing import Awaitable, Protocol, final

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


@final
@attrs.define(slots=True, frozen=True)
class StaticKeyDirectory:
    """Resolve every tenant to one fixed key reference (single-key deployments)."""

    key_ref: KeyRef
    """The single key-encryption-key reference used for all tenants."""

    async def resolve(self, tenant: TenantIdentity | None) -> KeyRef:
        _ = tenant
        return self.key_ref


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

    async def resolve(self, tenant: TenantIdentity | None) -> KeyRef:
        if tenant is None:
            return KeyRef(key_id=self.default_key_id, version=self.version)

        return KeyRef(
            key_id=self.template.format(tenant_id=tenant.tenant_id),
            version=self.version,
        )
