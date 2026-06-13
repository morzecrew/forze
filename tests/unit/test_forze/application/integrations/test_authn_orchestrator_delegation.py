"""Orchestrator API-key delegation: an ``act`` claim becomes ``AuthnIdentity.actor``.

API-key delegation is *intrinsic* — honored without configuring ``actor_claim`` (the
framework mints those keys, so a carried actor is first-party). The token path stays
opt-in, gated on ``actor_claim``; both are asserted here.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authn import (
    ACT_CLAIM,
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnIdentity,
    VerifiedAssertion,
)
from forze.application.integrations.authn import AuthnOrchestrator

pytestmark = pytest.mark.unit

# ----------------------- #

_USER = uuid4()
_AGENT = uuid4()


class _Resolver:
    async def resolve(self, assertion: VerifiedAssertion) -> AuthnIdentity:
        return AuthnIdentity(principal_id=UUID(assertion.subject))


class _Eligibility:
    async def require_authentication_allowed(self, principal_id: UUID) -> None:
        _ = principal_id


class _StubApiKeyVerifier:
    def __init__(self, *, with_agent: bool) -> None:
        self._with_agent = with_agent

    async def verify_api_key(self, credentials: ApiKeyCredentials) -> VerifiedAssertion:
        _ = credentials
        claims = {ACT_CLAIM: {"sub": str(_AGENT)}} if self._with_agent else {}
        return VerifiedAssertion(issuer="test", subject=str(_USER), claims=claims)


class _StubTokenVerifier:
    async def verify_token(self, credentials: AccessTokenCredentials) -> VerifiedAssertion:
        _ = credentials
        return VerifiedAssertion(
            issuer="test",
            subject=str(_USER),
            claims={ACT_CLAIM: {"sub": str(_AGENT)}},
        )


def _api_key_orchestrator(*, with_agent: bool) -> AuthnOrchestrator:
    return AuthnOrchestrator(
        resolver=_Resolver(),
        eligibility=_Eligibility(),
        enabled_methods=frozenset({"api_key"}),
        api_key_verifier=_StubApiKeyVerifier(with_agent=with_agent),
        # No actor_claim configured — the api-key path must still honor delegation.
    )


# ....................... #


class TestApiKeyDelegation:
    @pytest.mark.asyncio
    async def test_delegation_key_attaches_agent_as_actor(self) -> None:
        result = await _api_key_orchestrator(with_agent=True).authenticate_with_api_key(
            ApiKeyCredentials(key="k")
        )

        assert result.identity.principal_id == _USER  # effective subject
        assert result.identity.actor is not None
        assert result.identity.actor.principal_id == _AGENT  # delegation agent
        assert result.identity.is_delegated is True

    @pytest.mark.asyncio
    async def test_plain_key_has_no_actor(self) -> None:
        result = await _api_key_orchestrator(with_agent=False).authenticate_with_api_key(
            ApiKeyCredentials(key="k")
        )

        assert result.identity.principal_id == _USER
        assert result.identity.actor is None


class TestTokenPathStaysOptIn:
    @pytest.mark.asyncio
    async def test_token_act_ignored_without_actor_claim(self) -> None:
        # Same act claim on a token is NOT honored unless actor_claim is configured.
        orchestrator = AuthnOrchestrator(
            resolver=_Resolver(),
            eligibility=_Eligibility(),
            enabled_methods=frozenset({"token"}),
            token_verifier=_StubTokenVerifier(),
        )

        result = await orchestrator.authenticate_with_token(
            AccessTokenCredentials(token="t")
        )

        assert result.identity.actor is None

    @pytest.mark.asyncio
    async def test_token_act_honored_when_actor_claim_set(self) -> None:
        orchestrator = AuthnOrchestrator(
            resolver=_Resolver(),
            eligibility=_Eligibility(),
            enabled_methods=frozenset({"token"}),
            token_verifier=_StubTokenVerifier(),
            actor_claim=ACT_CLAIM,
        )

        result = await orchestrator.authenticate_with_token(
            AccessTokenCredentials(token="t")
        )

        assert result.identity.actor is not None
        assert result.identity.actor.principal_id == _AGENT
