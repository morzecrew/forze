"""API-key bearer auth for the MCP boundary, backed by the forze_identity brain.

The MCP server is a **Resource Server**: it validates an inbound bearer credential
and binds the resulting principal — it does not run an OAuth flow. The bearer is a
forze_identity **API key** the holder already has (a controlled agent's secret, or a
key the user minted in your web UI and pasted into the agent host), so there is no
authorization handshake to implement.

Two pieces, mirroring the FastAPI edge so both transports authenticate identically:

- :class:`ForzeApiKeyVerifier` plugs into ``FastMCP(auth=...)``. It runs the inbound
  key through the **same** :class:`AuthnOrchestrator` the HTTP edge uses
  (``authenticate_with_api_key``), resolves the tenant, and hands FastMCP an
  ``AccessToken`` (rejecting an invalid key with ``None`` → a clean ``401`` +
  ``WWW-Authenticate``). Verification happens once, here.
- :class:`AccessTokenIdentityResolver` is the ``MCPIdentityResolver`` the dispatch
  path calls. It reads the already-verified token from FastMCP's request context and
  rebuilds the :class:`AuthnIdentity` / :class:`TenantIdentity` to bind — attaching a
  fixed **agent** service principal as the delegation ``actor`` (so the engine's
  least-privilege intersection of user×agent grants applies, the confused-deputy
  defense). No re-verification.

Read-only is the default at the tool-exposure level (``register_tools(include_writes=
False)``); finer per-agent ceilings come from the agent principal's authz grants.

The agent (delegation ``actor``) can come from the key itself: a key minted for a
user→agent pair (``issue_api_key(..., actor_principal_id=...)``) carries its agent,
so per-connection keys attribute to their own agent and revoke independently. The
``agent=`` on :class:`AccessTokenIdentityResolver` is the **fallback** when the key
carries none — pass it to give plain (non-delegation) keys a fixed agent, or omit it
to bind the bare user.
"""

from uuid import UUID

import attrs
from fastmcp.server.auth import AccessToken, TokenVerifier
from fastmcp.server.dependencies import get_access_token

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    AuthnResult,
    AuthnSpec,
)
from forze.application.contracts.authn.value_objects import AuthnIdentity
from forze.application.contracts.tenancy import (
    TenantIdentity,
    parse_tenant_hint,
)
from forze.application.execution.context import (
    ExecutionContext,
    ExecutionContextFactory,
)
from forze.base.exceptions import CoreException, ExceptionKind

from .identity import MCPIdentityResolver

# ----------------------- #

_TENANT_ID_CLAIM = "tid"
"""AccessToken claim carrying the resolved tenant id (string UUID)."""

_TENANT_KEY_CLAIM = "tkey"
"""AccessToken claim carrying the resolved tenant key, when present."""

_AGENT_CLAIM = "agent"
"""AccessToken claim carrying the delegation agent principal id (string UUID), when
the verified key is a user→agent delegation. Bridges the agent the orchestrator
resolved (from the key) to the resolver, which prefers it over any fixed agent."""

# ....................... #


def _split_api_key(raw: str) -> ApiKeyCredentials:
    """Split a ``prefix:secret`` bearer into credentials (mirrors the HTTP edge).

    A key with no ``:`` separator is taken whole as the secret (no prefix), so the
    same key authenticates over FastAPI and MCP.
    """

    head, _, tail = raw.strip().partition(":")

    if not tail:
        return ApiKeyCredentials(key=head)

    return ApiKeyCredentials(key=tail, prefix=head)


# ....................... #


async def _resolve_tenant(
    ctx: ExecutionContext,
    result: AuthnResult,
) -> TenantIdentity | None:
    """Resolve the tenant for an authenticated principal (mirrors the HTTP edge).

    Prefers a tenancy resolver (principal→tenant, validating any issuer hint); falls
    back to a tenant carried on the verified credential. No untrusted header path —
    MCP clients present only the bearer.
    """

    requested = parse_tenant_hint(result.issuer_tenant_hint)
    resolver = ctx.tenancy.resolver()

    if resolver is not None:
        return await resolver.resolve_from_principal(
            result.identity.principal_id,
            requested_tenant_id=requested,
        )

    return TenantIdentity(tenant_id=requested) if requested is not None else None


# ....................... #


@attrs.define(slots=True, eq=False)
class ForzeApiKeyVerifier(TokenVerifier):
    """``FastMCP(auth=...)`` provider validating a forze_identity API-key bearer.

    Verifies the key through the configured :class:`AuthnSpec`'s orchestrator and
    returns an ``AccessToken`` carrying the principal (``subject``) and resolved
    tenant (claims); an invalid key returns ``None`` so FastMCP answers ``401``.

    :param ctx_factory: Yields an execution context to resolve the authn/tenancy
        ports (the same factory the MCP server dispatches with).
    :param authn_spec: Selects the authn route/profile to verify against; its
        ``name`` is the deps route (e.g. ``AuthnSpec(name="main")``).
    :param client_id: Opaque client id stamped on the ``AccessToken`` (audit only).
    """

    ctx_factory: ExecutionContextFactory
    authn_spec: AuthnSpec
    client_id: str = "forze-mcp"

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # TokenVerifier is a plain (non-attrs) base with its own __init__; initialise
        # it explicitly since attrs does not chain to it.
        TokenVerifier.__init__(self)

    # ....................... #

    async def verify_token(self, token: str) -> AccessToken | None:
        ctx = self.ctx_factory()

        # Port resolution outside the catch: a misconfigured spec/route is a
        # deployment error, not a bad credential — it must fail loud, not 401.
        authn = ctx.authn.authn(self.authn_spec)

        try:
            result = await authn.authenticate_with_api_key(_split_api_key(token))

        except CoreException as error:
            # Reject only on an authentication failure (unknown/expired/inactive
            # key); let infrastructure/config errors propagate.
            if error.kind is ExceptionKind.AUTHENTICATION:
                return None

            raise

        if result is None:  # pyright: ignore[reportUnnecessaryComparison]
            return None

        tenant = await _resolve_tenant(ctx, result)
        claims: dict[str, object] = {}

        if tenant is not None:
            claims[_TENANT_ID_CLAIM] = str(tenant.tenant_id)

            if tenant.tenant_key is not None:
                claims[_TENANT_KEY_CLAIM] = tenant.tenant_key

        # A delegation key resolves to an identity carrying its agent as actor; carry
        # that agent's principal id so the resolver attaches it (single-hop in v1).
        if result.identity.actor is not None:
            claims[_AGENT_CLAIM] = str(result.identity.actor.principal_id)

        return AccessToken(
            token=token,
            client_id=self.client_id,
            scopes=[],
            subject=str(result.identity.principal_id),
            claims=claims,
        )


# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class AccessTokenIdentityResolver(MCPIdentityResolver):
    """Bind the identity FastMCP already verified, attaching a fixed agent actor.

    Reads the request-scoped ``AccessToken`` (set by :class:`ForzeApiKeyVerifier`),
    reconstructs the principal and tenant, and — when an *agent* is configured —
    attaches it as the delegation ``actor`` so the engine enforces the least-privilege
    intersection of the user's and the agent's grants.

    :param agent: A **fallback** agent service principal attached as ``actor`` when
        the verified key does not carry its own agent. A delegation key (one minted
        for a user→agent pair) carries its agent on the credential and takes
        precedence over this. ``None`` binds the bare user identity when the key
        carries no agent either.
    """

    agent: AuthnIdentity | None = None

    # ....................... #

    async def resolve(self) -> tuple[AuthnIdentity | None, TenantIdentity | None]:
        token = get_access_token()

        if token is None or token.subject is None:
            return None, None

        identity = AuthnIdentity(principal_id=UUID(token.subject))

        # A key-carried agent (a user→agent delegation key) wins over the fixed
        # fallback, so per-connection keys attribute to their own agent.
        key_agent = token.claims.get(_AGENT_CLAIM)
        actor = (
            AuthnIdentity(principal_id=UUID(key_agent))
            if isinstance(key_agent, str)
            else self.agent
        )

        if actor is not None:
            identity = attrs.evolve(identity, actor=actor)

        tenant: TenantIdentity | None = None
        tenant_id = token.claims.get(_TENANT_ID_CLAIM)

        if isinstance(tenant_id, str):
            tenant = TenantIdentity(
                tenant_id=UUID(tenant_id),
                tenant_key=token.claims.get(_TENANT_KEY_CLAIM),
            )

        return identity, tenant
