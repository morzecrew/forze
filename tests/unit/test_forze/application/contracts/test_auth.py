"""Tests for authentication and authorization contracts (value objects, deps, ports)."""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import pytest

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    ApiKeyLifecycleDepKey,
    ApiKeyResponse,
    AuthnDepKey,
    AuthnIdentity,
    AuthnSpec,
    OAuth2Tokens,
    OAuth2TokensResponse,
    PasswordCredentials,
    TokenCredentials,
    TokenLifecycleDepKey,
    TokenResponse,
)
from forze.application.contracts.authn.value_objects import CredentialLifetime
from forze.application.contracts.authn.ports import (
    ApiKeyLifecyclePort,
    AuthnPort,
    TokenLifecyclePort,
)
from forze.application.contracts.authz import AuthzDepKey, coalesce_authz_tenant_id
from forze.application.contracts.authz.ports import AuthzPort
from forze.application.contracts.authz.value_objects import PrincipalRef
from forze.base.errors import CoreError


class TestAuthnSpec:
    def test_minimal_spec(self) -> None:
        spec = AuthnSpec(name="auth")
        assert spec.name == "auth"


class TestAuthnIdentity:
    def test_principal_only(self) -> None:
        pid = uuid4()
        ident = AuthnIdentity(principal_id=pid)
        assert ident.principal_id == pid


class TestCredentialsAndTokens:
    def test_password_credentials(self) -> None:
        cred = PasswordCredentials(login="user", password="secret")
        assert cred.login == "user"
        assert cred.password == "secret"

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

    def test_oauth2_tokens_optional_refresh(self) -> None:
        access = TokenCredentials(token="a")
        refresh = TokenCredentials(token="r")
        bundle = OAuth2Tokens(
            access_token=access,
            refresh_token=refresh,
        )
        assert bundle.refresh_token is refresh

    def test_nested_token_and_api_key_responses(self) -> None:
        key_cred = ApiKeyCredentials(key="secret")
        api_resp = ApiKeyResponse(
            key=key_cred,
            lifetime=CredentialLifetime(expires_in=timedelta(hours=1)),
        )
        assert api_resp.key is key_cred
        assert api_resp.lifetime is not None
        assert api_resp.lifetime.expires_in == timedelta(hours=1)

        tok = TokenResponse(
            token=TokenCredentials(token="x"),
            lifetime=None,
        )
        oauth_resp = OAuth2TokensResponse(
            access_token=tok,
            refresh_token=None,
        )
        assert oauth_resp.refresh_token is None


class TestAuthnAndAuthzDepKeys:
    def test_dep_key_names_are_stable(self) -> None:
        assert AuthnDepKey.name == "authn"
        assert AuthzDepKey.name == "authz"
        assert TokenLifecycleDepKey.name == "authn_token_lifecycle"
        assert ApiKeyLifecycleDepKey.name == "authn_api_key_lifecycle"


class TestCoalesceAuthzTenantId:
    def test_explicit_wins_over_unscoped_ref(self) -> None:
        tid = uuid4()
        pid = uuid4()
        ref = PrincipalRef(principal_id=pid, kind="user")

        assert coalesce_authz_tenant_id(ref, tenant_id=tid) == tid

    def test_ref_tenant_when_explicit_none(self) -> None:
        tid = uuid4()
        pid = uuid4()
        ref = PrincipalRef(principal_id=pid, kind="user", tenant_id=tid)

        assert coalesce_authz_tenant_id(ref, tenant_id=None) == tid

    def test_matching_explicit_and_ref(self) -> None:
        tid = uuid4()
        pid = uuid4()
        ref = PrincipalRef(principal_id=pid, kind="user", tenant_id=tid)

        assert coalesce_authz_tenant_id(ref, tenant_id=tid) == tid

    def test_uuid_principal_uses_explicit_only(self) -> None:
        tid = uuid4()
        pid = uuid4()

        assert coalesce_authz_tenant_id(pid, tenant_id=tid) == tid
        assert coalesce_authz_tenant_id(pid, tenant_id=None) is None

    def test_conflict_raises(self) -> None:
        pid = uuid4()
        ref = PrincipalRef(principal_id=pid, kind="user", tenant_id=uuid4())

        with pytest.raises(CoreError, match="Conflicting tenant_id"):
            coalesce_authz_tenant_id(ref, tenant_id=uuid4())


def _pid_from_str(value: str) -> UUID:
    return uuid5(NAMESPACE_URL, value)


class _StubAuthenticationPort:
    async def authenticate_with_password(self, credentials: PasswordCredentials) -> AuthnIdentity:
        return AuthnIdentity(principal_id=_pid_from_str("pw:" + credentials.login))

    async def authenticate_with_token(self, credentials: TokenCredentials) -> AuthnIdentity:
        return AuthnIdentity(principal_id=_pid_from_str("tok:" + credentials.token))

    async def authenticate_with_api_key(self, credentials: ApiKeyCredentials) -> AuthnIdentity:
        return AuthnIdentity(principal_id=_pid_from_str("key:" + credentials.key))


class _StubTokenLifecyclePort:
    async def issue_tokens(self, identity: AuthnIdentity) -> OAuth2TokensResponse:
        tr = TokenResponse(token=TokenCredentials(token="issued"))
        return OAuth2TokensResponse(access_token=tr)

    async def refresh_tokens(self, credentials: OAuth2Tokens) -> OAuth2TokensResponse:
        tr = TokenResponse(token=TokenCredentials(token="refreshed"))
        return OAuth2TokensResponse(access_token=tr)

    async def revoke_tokens(self, identity: AuthnIdentity) -> None:
        return None


class _StubApiKeyLifecyclePort:
    async def issue_api_key(self, identity: AuthnIdentity) -> ApiKeyResponse:
        return ApiKeyResponse(key=ApiKeyCredentials(key="issued"))

    async def refresh_api_key(self, credentials: ApiKeyCredentials) -> ApiKeyResponse:
        return ApiKeyResponse(key=credentials)

    async def revoke_api_key(self, key_id: str) -> None:
        return None

    async def revoke_many_api_keys(self, key_ids: tuple[str, ...]) -> None:
        return None


class _StubAuthzPort:
    async def permits(
        self,
        principal: Any,
        permission_key: str,
        *,
        tenant_id: UUID | None = None,
        resource: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> bool:
        _ = permission_key, resource, context, tenant_id, principal

        return True


@pytest.mark.asyncio
async def test_authentication_port_stub_round_trip() -> None:
    port: AuthnPort = _StubAuthenticationPort()
    pw = await port.authenticate_with_password(
        PasswordCredentials(login="alice", password="x"),
    )
    assert pw.principal_id == _pid_from_str("pw:alice")
    tok = await port.authenticate_with_token(TokenCredentials(token="t"))
    assert tok.principal_id == _pid_from_str("tok:t")
    key = await port.authenticate_with_api_key(ApiKeyCredentials(key="k"))
    assert key.principal_id == _pid_from_str("key:k")


@pytest.mark.asyncio
async def test_token_lifecycle_port_stub() -> None:
    port: TokenLifecyclePort = _StubTokenLifecyclePort()
    ident = AuthnIdentity(principal_id=uuid4())
    issued = await port.issue_tokens(ident)
    assert issued.access_token.token.token == "issued"
    refreshed = await port.refresh_tokens(OAuth2Tokens(access_token=TokenCredentials(token="a")))
    assert refreshed.access_token.token.token == "refreshed"
    await port.revoke_tokens(ident)


@pytest.mark.asyncio
async def test_api_key_lifecycle_port_stub() -> None:
    port: ApiKeyLifecyclePort = _StubApiKeyLifecyclePort()
    ident = AuthnIdentity(principal_id=uuid4())
    issued = await port.issue_api_key(ident)
    assert issued.key.key == "issued"
    assert await port.refresh_api_key(ApiKeyCredentials(key="k")) is not None
    await port.revoke_api_key("kid")
    await port.revoke_many_api_keys(("x",))


@pytest.mark.asyncio
async def test_authz_port_stub() -> None:
    port: AuthzPort = _StubAuthzPort()
    ident = AuthnIdentity(principal_id=uuid4())
    assert await port.permits(ident, "read") is True
