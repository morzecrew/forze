"""Boundary identity resolution for MCP tool calls.

An MCP tool call is a driving-adapter entrypoint, so it must establish an execution
context just like the FastAPI middleware does. A :class:`MCPIdentityResolver` turns an
incoming call into the ``(authn, tenant)`` pair bound onto the invocation context before
the operation runs.

:class:`StaticIdentityResolver` binds a fixed, configured identity (or none — for local,
unauthenticated read-only servers). :class:`DelegatedIdentityResolver` runs each call on
behalf of a user resolved from the session while attaching the server's own service
principal as the :attr:`~forze.application.contracts.authn.AuthnIdentity.actor`, so the
engine enforces least-privilege intersection of user and agent grants. Production wiring —
extracting a verified token from the MCP session and mapping it through
``PrincipalResolverPort`` / ``TenantResolverPort`` — plugs in behind ``resolve_subject``.
"""

from collections.abc import Awaitable, Callable
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


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DelegatedIdentityResolver(MCPIdentityResolver):
    """Resolve the *subject* (user) from the call and attach a fixed *actor* (the agent).

    The MCP server runs as its own service principal (``agent``); each tool call is performed
    on behalf of a user resolved from the session (``resolve_subject`` — e.g. mapping the
    verified token). The resulting identity carries the user as the effective subject and the
    agent as :attr:`~forze.application.contracts.authn.AuthnIdentity.actor`, so the engine
    enforces least-privilege intersection of their grants (the confused-deputy defense).
    """

    agent: AuthnIdentity
    """The MCP server's own service principal (the delegate performing the calls)."""

    resolve_subject: Callable[
        [], Awaitable[tuple[AuthnIdentity, TenantIdentity | None]]
    ]
    """Resolve the on-behalf-of user (subject) and tenant for the current call."""

    # ....................... #

    async def resolve(self) -> tuple[AuthnIdentity | None, TenantIdentity | None]:
        """Resolve the subject and attach the agent as its actor."""

        subject, tenant = await self.resolve_subject()

        return attrs.evolve(subject, actor=self.agent), tenant
