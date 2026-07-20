"""Tests for authentication and authorization contracts (value objects, deps, ports)."""

from __future__ import annotations

from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnSpec,
    IssuedAccessToken,
    IssuedApiKey,
    IssuedInvite,
    IssuedRefreshToken,
    IssuedTokens,
    PasswordCredentials,
    RefreshTokenCredentials,
)
from forze.application.contracts.authn.ports import AuthnPort
from forze.application.contracts.authz import (
    AuthzDecision,
    AuthzDecisionDepKey,
    AuthzRequest,
    AuthzScope,
    AuthzSubject,
    GrantQueryDepKey,
    resolve_policy_scope,
    subject_for_grant_query,
    subject_from_authn,
)
from forze.application.contracts.authz.specs import AuthzSpec
from forze.base.exceptions import CoreException

# ----------------------- #


class TestAuthnSpec:
    def test_minimal_spec(self) -> None:
        spec = AuthnSpec(name="auth")
        assert spec.name == "auth"


class TestCredentialReprMasking:
    """Secret-bearing fields must never leak through ``repr`` (logs, tracebacks)."""

    def test_password_credentials_repr_hides_password(self) -> None:
        creds = PasswordCredentials(login="alice", password="s3cr3t-password")
        assert "s3cr3t-password" not in repr(creds)
        assert "alice" in repr(creds)

    def test_api_key_credentials_repr_hides_key(self) -> None:
        creds = ApiKeyCredentials(key="s3cr3t-api-key", prefix="fz")
        assert "s3cr3t-api-key" not in repr(creds)
        assert "fz" in repr(creds)

    def test_access_token_credentials_repr_hides_token(self) -> None:
        creds = AccessTokenCredentials(token="s3cr3t-access-token")
        assert "s3cr3t-access-token" not in repr(creds)
        assert "Bearer" in repr(creds)

    def test_refresh_token_credentials_repr_hides_token(self) -> None:
        creds = RefreshTokenCredentials(token="s3cr3t-refresh-token")
        assert "s3cr3t-refresh-token" not in repr(creds)

    def test_issued_invite_repr_hides_token(self) -> None:
        invite = IssuedInvite(token="s3cr3t-invite-token", principal_id=uuid4())
        assert "s3cr3t-invite-token" not in repr(invite)

    def test_issued_tokens_repr_hides_nested_secrets(self) -> None:
        issued = IssuedTokens(
            access=IssuedAccessToken(
                token=AccessTokenCredentials(token="s3cr3t-access-token")
            ),
            refresh=IssuedRefreshToken(
                token=RefreshTokenCredentials(token="s3cr3t-refresh-token")
            ),
        )
        rendered = repr(issued)
        assert "s3cr3t-access-token" not in rendered
        assert "s3cr3t-refresh-token" not in rendered

    def test_issued_api_key_repr_hides_nested_key(self) -> None:
        issued = IssuedApiKey(key=ApiKeyCredentials(key="s3cr3t-api-key"))
        assert "s3cr3t-api-key" not in repr(issued)


class TestAuthnIdentity:
    def test_principal_only(self) -> None:
        pid = uuid4()
        ident = AuthnIdentity(principal_id=pid)
        assert ident.principal_id == pid
        assert ident.actor is None
        assert ident.is_delegated is False

    def test_delegated_carries_actor(self) -> None:
        agent = AuthnIdentity(principal_id=uuid4())
        user = AuthnIdentity(principal_id=uuid4(), actor=agent)
        assert user.is_delegated is True
        assert user.actor is agent


class TestResolveAuthzScope:
    def test_explicit_tenant(self) -> None:
        tid = uuid4()
        spec = AuthzSpec(name="z")

        scope = resolve_policy_scope(
            spec=spec,
            explicit=AuthzScope(tenant_id=tid),
            invocation_tenant_id=None,
        )

        assert scope.tenant_id == tid

    def test_require_invocation_tenant(self) -> None:
        tid = uuid4()
        spec = AuthzSpec(name="z", tenancy_mode="require_invocation_tenant")

        scope = resolve_policy_scope(
            spec=spec,
            explicit=AuthzScope(tenant_id=tid),
            invocation_tenant_id=tid,
        )

        assert scope.tenant_id == tid

    def test_conflict_raises(self) -> None:
        spec = AuthzSpec(name="z", tenancy_mode="require_invocation_tenant")

        with pytest.raises(CoreException, match="disagree"):
            resolve_policy_scope(
                spec=spec,
                explicit=AuthzScope(tenant_id=uuid4()),
                invocation_tenant_id=uuid4(),
            )


class TestSubjectHelpers:
    def test_subject_from_authn(self) -> None:
        pid = uuid4()
        subject = subject_from_authn(AuthnIdentity(principal_id=pid))
        assert subject.principal_id == pid
        assert subject.actor is None

    def test_subject_from_authn_carries_actor_chain(self) -> None:
        agent_pid = uuid4()
        user_pid = uuid4()
        identity = AuthnIdentity(
            principal_id=user_pid, actor=AuthnIdentity(principal_id=agent_pid)
        )
        subject = subject_from_authn(identity)
        assert subject.principal_id == user_pid
        assert subject.actor is not None
        assert subject.actor.principal_id == agent_pid

    def test_subject_from_authn_rejects_too_deep_chain(self) -> None:
        from forze.application.contracts.authz import MAX_DELEGATION_DEPTH

        ident = AuthnIdentity(principal_id=uuid4())
        for _ in range(MAX_DELEGATION_DEPTH + 2):
            ident = AuthnIdentity(principal_id=uuid4(), actor=ident)

        with pytest.raises(CoreException) as exc_info:
            subject_from_authn(ident)
        assert exc_info.value.code == "delegation_chain_too_deep"

    def test_subject_for_grant_query_variants(self) -> None:
        pid = uuid4()
        assert subject_for_grant_query(pid) == pid
        assert subject_for_grant_query(AuthnIdentity(principal_id=pid)) == pid
        assert subject_for_grant_query(AuthzSubject(principal_id=pid)) == pid


class TestAuthnAndAuthzDepKeys:
    def test_dep_key_names_are_stable(self) -> None:
        assert AuthnDepKey.name == "authn"
        assert AuthzDecisionDepKey.name == "authz_decision"
        assert GrantQueryDepKey.name == "authz_grant_query"


def _pid_from_str(value: str) -> UUID:
    return uuid5(NAMESPACE_URL, value)


class _StubAuthenticationPort:
    async def authenticate_with_password(
        self,
        credentials: PasswordCredentials,
    ) -> AuthnIdentity:
        return AuthnIdentity(principal_id=_pid_from_str("pw:" + credentials.login))

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnIdentity:
        return AuthnIdentity(principal_id=_pid_from_str("tok:" + credentials.token))

    async def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> AuthnIdentity:
        return AuthnIdentity(principal_id=_pid_from_str("key:" + credentials.key))


class _StubTokenLifecyclePort:
    async def issue_tokens(self, identity: AuthnIdentity) -> IssuedTokens:
        return IssuedTokens(
            access=IssuedAccessToken(token=AccessTokenCredentials(token="issued")),
        )

    async def refresh_tokens(
        self,
        refresh_token: RefreshTokenCredentials,
    ) -> IssuedTokens:
        _ = refresh_token

        return IssuedTokens(
            access=IssuedAccessToken(token=AccessTokenCredentials(token="refreshed")),
        )

    async def revoke_tokens(self, identity: AuthnIdentity) -> None:
        return None


class _StubApiKeyLifecyclePort:
    async def issue_api_key(self, identity: AuthnIdentity) -> IssuedApiKey:
        return IssuedApiKey(key=ApiKeyCredentials(key="issued"))

    async def refresh_api_key(self, credentials: ApiKeyCredentials) -> IssuedApiKey:
        return IssuedApiKey(key=credentials)

    async def revoke_api_key(self, identity: AuthnIdentity, key_id: str) -> None:
        _ = identity, key_id
        return None

    async def revoke_many_api_keys(
        self,
        identity: AuthnIdentity,
        key_ids: tuple[str, ...],
    ) -> None:
        _ = identity, key_ids
        return None


class _StubRuntimePort:
    async def authorize(self, request: AuthzRequest) -> AuthzDecision:
        _ = request
        return AuthzDecision(allowed=True, matched_permission_key="read")


@pytest.mark.asyncio
async def test_authentication_port_stub_round_trip() -> None:
    port: AuthnPort = _StubAuthenticationPort()
    pw = await port.authenticate_with_password(
        PasswordCredentials(login="alice", password="x"),
    )
    assert pw.principal_id == _pid_from_str("pw:alice")


@pytest.mark.asyncio
async def test_runtime_port_stub() -> None:
    port = _StubRuntimePort()
    ident = AuthnIdentity(principal_id=uuid4())
    decision = await port.authorize(
        AuthzRequest(
            subject=subject_from_authn(ident),
            action="documents.read",
        ),
    )
    assert decision.allowed is True
