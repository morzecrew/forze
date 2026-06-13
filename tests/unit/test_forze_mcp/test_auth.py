"""API-key MCP auth: verifier (orchestrator-backed) + identity resolver.

The verifier is exercised against a stub ``AuthnPort`` (the real forze_identity
verifiers are tested under ``test_forze_identity``); these tests pin *this* layer:
the key→``AccessToken`` mapping, tenant resolution, the authentication-only reject
posture, and the resolver rebuilding the bound identity with a fixed agent actor.
"""

from __future__ import annotations

from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

pytest.importorskip("fastmcp")

from fastmcp.server.auth import AccessToken

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    AuthnDepKey,
    AuthnResult,
    AuthnSpec,
)
from forze.application.contracts.authn.value_objects import AuthnIdentity
from forze.application.execution import Deps
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze_mcp import AccessTokenIdentityResolver, ForzeApiKeyVerifier
from forze_mcp import auth as auth_mod
from tests.support.execution_context import context_from_deps

# ----------------------- #

_GOOD_KEY = "demo-key-abc123"
_PID = uuid5(NAMESPACE_URL, "principal")
_TID = UUID("f47ac10b-58cc-4372-a567-0e02b2c3d479")
_SPEC = AuthnSpec(name="auth", enabled_methods=frozenset({"api_key"}))


class _StubApiKeyPort:
    """Minimal AuthnPort: accepts ``_GOOD_KEY``, rejects others; optional hint/boom/agent."""

    def __init__(
        self,
        *,
        issuer_tenant_hint: str | None = None,
        boom: bool = False,
        agent: UUID | None = None,
    ) -> None:
        self._hint = issuer_tenant_hint
        self._boom = boom
        self._agent = agent

    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult:
        assert isinstance(credentials, ApiKeyCredentials)

        if self._boom:
            raise exc.internal("dependency exploded")

        if credentials.key != _GOOD_KEY:
            raise exc.authentication("unknown api key")

        # A delegation key resolves to an identity carrying its agent as actor.
        actor = AuthnIdentity(principal_id=self._agent) if self._agent else None

        return AuthnResult(
            identity=AuthnIdentity(principal_id=_PID, actor=actor),
            issuer_tenant_hint=self._hint,
        )


def _ctx_factory(**kw: object):
    port = _StubApiKeyPort(**kw)  # type: ignore[arg-type]

    def _factory(ctx: ExecutionContext, spec: AuthnSpec) -> _StubApiKeyPort:
        return port

    return lambda: context_from_deps(Deps.plain({AuthnDepKey: _factory}))


def _verifier(**kw: object) -> ForzeApiKeyVerifier:
    return ForzeApiKeyVerifier(ctx_factory=_ctx_factory(**kw), authn_spec=_SPEC)


# ....................... #


class TestForzeApiKeyVerifier:
    @pytest.mark.asyncio
    async def test_valid_key_yields_access_token_with_principal(self) -> None:
        token = await _verifier().verify_token(_GOOD_KEY)

        assert token is not None
        assert token.subject == str(_PID)
        assert "tid" not in token.claims  # no tenant hint → no tenant claim

    @pytest.mark.asyncio
    async def test_tenant_hint_lands_on_claims(self) -> None:
        token = await _verifier(issuer_tenant_hint=str(_TID)).verify_token(_GOOD_KEY)

        assert token is not None
        assert token.claims["tid"] == str(_TID)

    @pytest.mark.asyncio
    async def test_prefixed_key_is_split_like_the_http_edge(self) -> None:
        # "prefix:secret" → the secret is what the verifier matches.
        token = await _verifier().verify_token(f"pfx:{_GOOD_KEY}")

        assert token is not None
        assert token.subject == str(_PID)

    @pytest.mark.asyncio
    async def test_unknown_key_is_rejected_with_none(self) -> None:
        assert await _verifier().verify_token("not-a-real-key") is None

    @pytest.mark.asyncio
    async def test_non_authentication_error_propagates(self) -> None:
        # A config/infra failure must fail loud, not masquerade as a bad key.
        with pytest.raises(Exception, match="dependency exploded"):
            await _verifier(boom=True).verify_token(_GOOD_KEY)

    @pytest.mark.asyncio
    async def test_delegation_key_carries_its_agent(self) -> None:
        agent = uuid4()
        token = await _verifier(agent=agent).verify_token(_GOOD_KEY)

        assert token is not None
        assert token.claims["agent"] == str(agent)


# ....................... #


class TestAccessTokenIdentityResolver:
    @pytest.mark.asyncio
    async def test_rebuilds_identity_and_attaches_agent_actor(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        agent = AuthnIdentity(principal_id=uuid4())
        token = AccessToken(
            token="t",
            client_id="c",
            scopes=[],
            subject=str(_PID),
            claims={"tid": str(_TID)},
        )
        monkeypatch.setattr(auth_mod, "get_access_token", lambda: token)

        identity, tenant = await AccessTokenIdentityResolver(agent=agent).resolve()

        assert identity is not None
        assert identity.principal_id == _PID  # effective subject = user
        assert identity.actor == agent  # actor = the deployment's agent
        assert tenant is not None
        assert tenant.tenant_id == _TID

    @pytest.mark.asyncio
    async def test_no_agent_binds_bare_user(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        token = AccessToken(token="t", client_id="c", scopes=[], subject=str(_PID), claims={})
        monkeypatch.setattr(auth_mod, "get_access_token", lambda: token)

        identity, tenant = await AccessTokenIdentityResolver().resolve()

        assert identity is not None
        assert identity.actor is None
        assert tenant is None

    @pytest.mark.asyncio
    async def test_key_carried_agent_wins_over_fixed_fallback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        key_agent = uuid4()
        fallback = AuthnIdentity(principal_id=uuid4())
        token = AccessToken(
            token="t",
            client_id="c",
            scopes=[],
            subject=str(_PID),
            claims={"agent": str(key_agent)},
        )
        monkeypatch.setattr(auth_mod, "get_access_token", lambda: token)

        identity, _ = await AccessTokenIdentityResolver(agent=fallback).resolve()

        assert identity is not None
        assert identity.actor is not None
        assert identity.actor.principal_id == key_agent  # the key's agent, not fallback

    @pytest.mark.asyncio
    async def test_unauthenticated_context_binds_nothing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(auth_mod, "get_access_token", lambda: None)

        assert await AccessTokenIdentityResolver().resolve() == (None, None)


# ....................... #


class TestVerifierResolverCompose:
    @pytest.mark.asyncio
    async def test_verified_token_flows_into_bound_identity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The verifier's AccessToken is what FastMCP hands the handler context; the
        # resolver must reconstruct the same principal + tenant + agent from it.
        agent = AuthnIdentity(principal_id=uuid4())
        token = await _verifier(issuer_tenant_hint=str(_TID)).verify_token(_GOOD_KEY)
        assert token is not None

        monkeypatch.setattr(auth_mod, "get_access_token", lambda: token)
        identity, tenant = await AccessTokenIdentityResolver(agent=agent).resolve()

        assert identity is not None
        assert identity.principal_id == _PID
        assert identity.actor == agent
        assert tenant is not None
        assert tenant.tenant_id == _TID
