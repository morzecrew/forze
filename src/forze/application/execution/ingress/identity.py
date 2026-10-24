"""Boundary identity resolution for a generated surface's calls.

A generated surface (MCP tools, the operation CLI) is a driving-adapter entrypoint, so it
establishes an execution context just like the FastAPI middleware does. An
:class:`IngressIdentityResolver` turns an incoming call into the ``(authn, tenant)`` pair
bound onto the invocation context before the operation runs.
"""

from typing import Protocol, final, runtime_checkable

import attrs

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity

# ----------------------- #


@runtime_checkable
class IngressIdentityResolver(Protocol):
    """Resolve the principal/tenant to bind for a call."""

    async def resolve(self) -> tuple[AuthnIdentity | None, TenantIdentity | None]:
        """Return the ``(authn, tenant)`` to bind for the current call."""
        ...


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StaticIdentityResolver(IngressIdentityResolver):
    """Resolve a fixed identity for every call (dev / single-tenant / service principal).

    Defaults to binding nothing — useful for local, unauthenticated read-only servers.
    Supply ``authn`` / ``tenant`` to run every call as a fixed service principal in a
    fixed tenant.
    """

    authn: AuthnIdentity | None = None
    tenant: TenantIdentity | None = None

    # ....................... #

    async def resolve(self) -> tuple[AuthnIdentity | None, TenantIdentity | None]:
        """Return the configured identity pair."""

        return self.authn, self.tenant
