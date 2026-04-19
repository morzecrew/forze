"""Tests for authentication contracts (value objects, spec, dependency keys)."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.auth import (
    ApiKeyCredentials,
    ApiKeyLifecycleDepKey,
    ApiKeyResponse,
    AuthIdentity,
    AuthSpec,
    AuthenticationDepKey,
    AuthorizationDepKey,
    AuthorizationRequest,
    OAuth2Tokens,
    OAuth2TokensResponse,
    PasswordCredentials,
    TokenCredentials,
    TokenLifecycleDepKey,
    TokenResponse,
)
from forze.application.contracts.auth.ports import (
    ApiKeyLifecyclePort,
    AuthenticationPort,
    AuthorizationPort,
    TokenLifecyclePort,
)


class TestAuthSpec:
    def test_minimal_spec(self) -> None:
        spec = AuthSpec(name="auth")
        assert spec.name == "auth"


class TestAuthIdentity:
    def test_defaults_are_safe_for_empty_principal(self) -> None:
        ident = AuthIdentity(subject_id="sub-1")
        assert ident.actor_id is None
        assert ident.tenant_id is None
        assert ident.claims is None
        assert ident.roles == frozenset()
        assert ident.permissions == frozenset()
        assert ident.is_active is True

    def test_with_roles_and_claims(self) -> None:
        ident = AuthIdentity(
            subject_id="u1",
            actor_id="a1",
            tenant_id="t1",
            claims={"scope": "read"},
            roles=frozenset({"admin"}),
            permissions=frozenset({"doc:read"}),
            is_active=False,
        )
        assert ident.claims == {"scope": "read"}
        assert "admin" in ident.roles
        assert ident.is_active is False


class TestCredentialsAndTokens:
    def test_password_credentials_hashed_flag(self) -> None:
        raw = PasswordCredentials(login="user", password="secret", is_hashed=False)
        hashed = PasswordCredentials(login="user", password="bcrypt$...", is_hashed=True)
        assert raw.is_hashed is False
        assert hashed.is_hashed is True

    def test_api_key_credentials_optional_prefix(self) -> None:
        bare = ApiKeyCredentials(key="k")
        prefixed = ApiKeyCredentials(key="k", prefix="pk_live_")
        assert bare.prefix is None
        assert prefixed.prefix == "pk_live_"

    def test_token_credentials_scheme_and_kind(self) -> None:
        cred = TokenCredentials(
            token="t",
            scheme="Bearer",
            kind="access",
        )
        assert cred.scheme == "Bearer"
        assert cred.kind == "access"

    def test_oauth2_tokens_optional_refresh_and_id(self) -> None:
        access = TokenCredentials(token="a")
        refresh = TokenCredentials(token="r")
        id_tok = TokenCredentials(token="i")
        bundle = OAuth2Tokens(
            access_token=access,
            refresh_token=refresh,
            id_token=id_tok,
        )
        assert bundle.refresh_token is refresh
        assert bundle.id_token is id_tok

    def test_nested_token_and_api_key_responses(self) -> None:
        key_cred = ApiKeyCredentials(key="secret")
        api_resp = ApiKeyResponse(
            key=key_cred,
            expires_in=timedelta(hours=1),
            scopes=("read", "write"),
        )
        assert api_resp.scopes == ("read", "write")

        tok = TokenResponse(
            token=TokenCredentials(token="x"),
            expires_in=None,
        )
        oauth_resp = OAuth2TokensResponse(
            access_token=tok,
            refresh_token=None,
        )
        assert oauth_resp.refresh_token is None


class TestAuthorizationRequest:
    def test_action_only(self) -> None:
        req = AuthorizationRequest(action="delete")
        assert req.resource is None
        assert req.subject is None
        assert req.context is None

    def test_resource_and_context_for_abac(self) -> None:
        subject = object()
        req = AuthorizationRequest(
            action="update",
            resource="invoice",
            subject=subject,
            context={"ip": "10.0.0.1"},
        )
        assert req.subject is subject
        assert req.context == {"ip": "10.0.0.1"}


class TestAuthDepKeys:
    def test_dep_key_names_are_stable(self) -> None:
        assert AuthenticationDepKey.name == "authentication"
        assert AuthorizationDepKey.name == "authorization"
        assert TokenLifecycleDepKey.name == "token_lifecycle"
        assert ApiKeyLifecycleDepKey.name == "api_key_lifecycle"


class _StubAuthenticationPort:
    async def authenticate_with_password(self, credentials: PasswordCredentials):
        return AuthIdentity(subject_id=credentials.login)

    async def authenticate_with_token(self, credentials: TokenCredentials):
        return AuthIdentity(subject_id="from-token")

    async def authenticate_with_api_key(self, credentials: ApiKeyCredentials):
        return AuthIdentity(subject_id="from-key")


class _StubTokenLifecyclePort:
    async def issue_tokens(self, identity: AuthIdentity):
        return None

    async def refresh_tokens(self, credentials: OAuth2Tokens):
        return None

    async def revoke_token(self, token_id: str) -> None:
        return None

    async def revoke_many_tokens(self, token_ids: tuple[str, ...]) -> None:
        return None


class _StubApiKeyLifecyclePort:
    async def issue_api_key(self, identity: AuthIdentity):
        return None

    async def refresh_api_key(self, credentials: ApiKeyCredentials):
        return None

    async def revoke_api_key(self, key_id: str) -> None:
        return None

    async def revoke_many_api_keys(self, key_ids: tuple[str, ...]) -> None:
        return None


class _StubAuthorizationPort:
    async def authorize(self, identity: AuthIdentity, request: AuthorizationRequest):
        return True

    async def authorize_many(
        self,
        identity: AuthIdentity,
        requests: tuple[AuthorizationRequest, ...],
    ):
        return True


@pytest.mark.asyncio
async def test_authentication_port_stub_round_trip() -> None:
    port: AuthenticationPort = _StubAuthenticationPort()
    pw = await port.authenticate_with_password(
        PasswordCredentials(login="alice", password="x"),
    )
    assert pw is not None and pw.subject_id == "alice"
    tok = await port.authenticate_with_token(TokenCredentials(token="t"))
    assert tok is not None and tok.subject_id == "from-token"
    key = await port.authenticate_with_api_key(ApiKeyCredentials(key="k"))
    assert key is not None and key.subject_id == "from-key"


@pytest.mark.asyncio
async def test_token_lifecycle_port_stub() -> None:
    port: TokenLifecyclePort = _StubTokenLifecyclePort()
    ident = AuthIdentity(subject_id="u")
    assert await port.issue_tokens(ident) is None
    assert await port.refresh_tokens(OAuth2Tokens(access_token=TokenCredentials(token="a"))) is None
    await port.revoke_token("id")
    await port.revoke_many_tokens(("a", "b"))


@pytest.mark.asyncio
async def test_api_key_lifecycle_port_stub() -> None:
    port: ApiKeyLifecyclePort = _StubApiKeyLifecyclePort()
    ident = AuthIdentity(subject_id="u")
    assert await port.issue_api_key(ident) is None
    assert await port.refresh_api_key(ApiKeyCredentials(key="k")) is None
    await port.revoke_api_key("kid")
    await port.revoke_many_api_keys(("x",))


@pytest.mark.asyncio
async def test_authorization_port_stub() -> None:
    port: AuthorizationPort = _StubAuthorizationPort()
    ident = AuthIdentity(subject_id="u")
    ok = await port.authorize(ident, AuthorizationRequest(action="read"))
    assert ok is True
    ok_many = await port.authorize_many(
        ident,
        (AuthorizationRequest(action="read"),),
    )
    assert ok_many is True
