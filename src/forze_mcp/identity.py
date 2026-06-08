"""Boundary identity resolution for MCP tool calls.

An MCP tool call is a driving-adapter entrypoint, so it must establish an execution
context just like the FastAPI middleware does. A :class:`MCPIdentityResolver` turns an
incoming call into the ``(authn, tenant)`` pair bound onto the invocation context before
the operation runs.

The read-only MVP ships :class:`StaticIdentityResolver` (a fixed, configured identity or
none). Production wiring — extracting a verified token from the MCP session and mapping it
through ``PrincipalResolverPort`` / ``TenantResolverPort`` — is the delegated-identity
phase; this protocol is the seam it will plug into.
"""

from typing import Protocol, final, runtime_checkable

import attrs

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.tenancy import TenantIdentity

# ----------------------- #


@runtime_checkable
class MCPIdentityResolver(Protocol):
    """Resolve the principal/tenant to bind for an MCP tool call."""

    async def resolve(self) -> tuple[AuthnIdentity | None, TenantIdentity | None]:
        """Return the ``(authn, tenant)`` to bind for the current call."""
        ...


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class StaticIdentityResolver(MCPIdentityResolver):
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
