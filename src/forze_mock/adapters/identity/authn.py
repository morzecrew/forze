"""In-memory authn port stubs (verifiers, lifecycle, orchestrator)."""

from __future__ import annotations

from typing import Any, Sequence, final
from uuid import UUID

import attrs

from forze.application.contracts.authn import AuthnSpec
from forze.application.contracts.authn.ports.authn import AuthnPort
from forze.application.contracts.authn.ports.deactivation import (
    PrincipalDeactivationPort,
)
from forze.application.contracts.authn.ports.eligibility import PrincipalEligibilityPort
from forze.application.contracts.authn.ports.lifecycle import (
    ApiKeyLifecyclePort,
    PasswordLifecyclePort,
    TokenLifecyclePort,
)
from forze.application.contracts.authn.ports.provisioning import (
    PasswordAccountProvisioningPort,
)
from forze.application.contracts.authn.ports.resolution import PrincipalResolverPort
from forze.application.contracts.authn.ports.verification import (
    ApiKeyVerifierPort,
    PasswordVerifierPort,
    TokenVerifierPort,
)
from forze.application.contracts.authn.value_objects.assertion import VerifiedAssertion
from forze.application.contracts.authn.value_objects.credentials import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    PasswordCredentials,
    RefreshTokenCredentials,
)
from forze.application.contracts.authn.value_objects.identity import AuthnIdentity
from forze.application.contracts.authn.value_objects.tokens import (
    IssuedApiKey,
    IssuedTokens,
)
from forze.base.exceptions import exc
from forze_mock.state import MockState

# ----------------------- #


def _route_store(state: MockState, route: str) -> dict[str, Any]:
    identity = state.identity
    authn = identity.setdefault("authn", {})
    assert isinstance(authn, dict)  # nosec: B101
    return authn.setdefault(route, {})  # type: ignore[assignment]


def _assertion_from_store(entry: dict[str, Any]) -> VerifiedAssertion:
    return VerifiedAssertion(
        issuer=str(entry.get("issuer", "mock")),
        subject=str(entry["subject"]),
        audience=entry.get("audience"),
        issuer_tenant_hint=entry.get("issuer_tenant_hint"),
    )


@final
@attrs.define(slots=True, kw_only=True)
class MockPasswordVerifierPort(PasswordVerifierPort):
    state: MockState
    route: str = "main"

    async def verify_password(
        self,
        credentials: PasswordCredentials,
    ) -> VerifiedAssertion:
        store = _route_store(self.state, self.route)
        entry = store.get("passwords", {}).get(credentials.login)  # type: ignore[union-attr]
        if entry is None:
            raise exc.authentication("Invalid login or password")
        assert isinstance(entry, dict)  # nosec: B101
        return _assertion_from_store(entry)  # type: ignore[arg-type]


@final
@attrs.define(slots=True, kw_only=True)
class MockTokenVerifierPort(TokenVerifierPort):
    state: MockState
    route: str = "main"

    async def verify_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> VerifiedAssertion:
        store = _route_store(self.state, self.route)
        entry = store.get("tokens", {}).get(credentials.token)  # type: ignore[union-attr]
        if entry is None:
            raise exc.authentication("Invalid token")
        assert isinstance(entry, dict)  # nosec: B101
        return _assertion_from_store(entry)  # type: ignore[arg-type]


@final
@attrs.define(slots=True, kw_only=True)
class MockApiKeyVerifierPort(ApiKeyVerifierPort):
    state: MockState
    route: str = "main"

    async def verify_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> VerifiedAssertion:
        store = _route_store(self.state, self.route)
        entry = store.get("api_keys", {}).get(credentials.key)  # type: ignore[union-attr]
        if entry is None:
            raise exc.authentication("Invalid API key")
        assert isinstance(entry, dict)  # nosec: B101
        return _assertion_from_store(entry)  # type: ignore[arg-type]


@final
@attrs.define(slots=True, kw_only=True)
class MockPrincipalResolverPort(PrincipalResolverPort):
    state: MockState
    route: str = "main"

    async def resolve(self, assertion: VerifiedAssertion) -> AuthnIdentity:
        store = _route_store(self.state, self.route)
        mapping = store.setdefault("principal_map", {})
        assert isinstance(mapping, dict)  # nosec: B101

        key = assertion.subject
        if key in mapping:
            return AuthnIdentity(principal_id=UUID(str(mapping[key])))  # type: ignore[arg-type]
        pid = UUID(
            str(store.get("default_principal", "00000000-0000-4000-8000-000000000001"))
        )
        mapping[key] = str(pid)
        return AuthnIdentity(principal_id=pid)


@final
@attrs.define(slots=True, kw_only=True)
class MockPrincipalEligibilityPort(PrincipalEligibilityPort):
    async def require_authentication_allowed(self, principal_id: UUID) -> None:
        _ = principal_id


@final
@attrs.define(slots=True, kw_only=True)
class MockPrincipalDeactivationPort(PrincipalDeactivationPort):
    async def deactivate(self, principal_id: UUID) -> None:
        _ = principal_id


@final
@attrs.define(slots=True, kw_only=True)
class MockTokenLifecyclePort(TokenLifecyclePort):
    async def issue_tokens(
        self,
        identity: AuthnIdentity,
        *,
        tenant_id: UUID | None = None,
    ) -> IssuedTokens:
        _ = identity, tenant_id
        raise exc.internal("Mock token lifecycle issue_tokens not configured")

    async def refresh_tokens(
        self,
        refresh_token: RefreshTokenCredentials,
    ) -> IssuedTokens:
        _ = refresh_token
        raise exc.internal("Mock token lifecycle refresh_tokens not configured")

    async def revoke_tokens(self, identity: AuthnIdentity) -> None:
        _ = identity


@final
@attrs.define(slots=True, kw_only=True)
class MockPasswordLifecyclePort(PasswordLifecyclePort):
    async def change_password(
        self,
        identity: AuthnIdentity,
        new_password: str,
    ) -> None:
        _ = identity, new_password
        raise exc.internal("Mock password lifecycle not configured")


@final
@attrs.define(slots=True, kw_only=True)
class MockApiKeyLifecyclePort(ApiKeyLifecyclePort):
    async def issue_api_key(self, identity: AuthnIdentity) -> IssuedApiKey:
        _ = identity
        raise exc.internal("Mock API key lifecycle not configured")

    async def refresh_api_key(self, credentials: ApiKeyCredentials) -> IssuedApiKey:
        _ = credentials
        raise exc.internal("Mock API key lifecycle not configured")

    async def revoke_api_key(self, identity: AuthnIdentity, key_id: str) -> None:
        _ = identity, key_id

    async def revoke_many_api_keys(
        self,
        identity: AuthnIdentity,
        key_ids: Sequence[str],
    ) -> None:
        _ = identity, key_ids


@final
@attrs.define(slots=True, kw_only=True)
class MockPasswordAccountProvisioningPort(PasswordAccountProvisioningPort):
    async def register_with_password(
        self,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        _ = principal_id, credentials
        raise exc.internal("Mock password provisioning not configured")

    async def provision_password_account(
        self,
        operator: AuthnIdentity,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        _ = operator, principal_id, credentials
        raise exc.internal("Mock password provisioning not configured")

    async def accept_invite_with_password(
        self,
        invite_token: str,
        principal_id: UUID,
        credentials: PasswordCredentials,
    ) -> None:
        _ = invite_token, principal_id, credentials
        raise exc.internal("Mock password provisioning not configured")


@final
@attrs.define(slots=True, kw_only=True)
class MockAuthnPort(AuthnPort):
    spec: AuthnSpec

    async def authenticate_with_password(
        self,
        credentials: PasswordCredentials,
    ) -> Any:
        _ = credentials
        raise exc.internal("Mock authn password flow not configured")

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> Any:
        _ = credentials
        raise exc.internal("Mock authn token flow not configured")

    async def authenticate_with_api_key(
        self,
        credentials: ApiKeyCredentials,
    ) -> Any:
        _ = credentials
        raise exc.internal("Mock authn API key flow not configured")
