"""Unit tests for verifier ports, resolver implementations, and the orchestrator.

These cover the new seams introduced by the strategic authn refactor:

* :class:`VerifiedAssertion` is the only thing that flows from a verifier to a resolver.
* Resolvers are independently swappable and can be unit-tested in isolation.
* :class:`AuthnOrchestrator` enforces ``enabled_methods`` regardless of which verifiers
  happen to be wired.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

pytest.importorskip("argon2")

pytestmark = pytest.mark.unit

from forze.application.contracts.authn import (
    ApiKeyCredentials,
    PasswordCredentials,
    AccessTokenCredentials,
    AuthnIdentity,
    VerifiedAssertion,
)
from forze.base.errors import AuthenticationError, CoreError
from forze.base.primitives import uuid4 as deterministic_uuid4
from forze_authn import (
    AuthnOrchestrator,
    DeterministicUuidResolver,
    JwtNativeUuidResolver,
)
from forze_authn.resolvers.deterministic_uuid import derive_principal_id

# ----------------------- #


class TestVerifiedAssertion:
    def test_required_fields_only(self) -> None:
        a = VerifiedAssertion(issuer="forze:jwt", subject="abc")

        assert a.issuer == "forze:jwt"
        assert a.subject == "abc"
        assert a.audience is None
        assert a.issuer_tenant_hint is None
        assert a.claims == {}

    def test_full_payload_and_immutability(self) -> None:
        now = datetime.now(tz=UTC)
        a = VerifiedAssertion(
            issuer="https://issuer.example",
            subject="firebase-uid-1",
            audience="my-app",
            issuer_tenant_hint="tenant-7",
            issued_at=now,
            expires_at=now,
            claims={"role": "admin"},
        )

        assert a.audience == "my-app"
        assert a.issuer_tenant_hint == "tenant-7"
        assert a.claims["role"] == "admin"

        with pytest.raises(Exception):
            a.subject = "other"  # type: ignore[misc]


# ....................... #


class TestJwtNativeUuidResolver:
    @pytest.mark.asyncio
    async def test_uuid_subject_round_trip(self) -> None:
        pid = uuid4()
        a = VerifiedAssertion(issuer="forze:jwt", subject=str(pid))
        ident = await JwtNativeUuidResolver().resolve(a)

        assert ident.principal_id == pid
        assert not hasattr(ident, "tenant_id")

    @pytest.mark.asyncio
    async def test_uuid_subject_ignores_issuer_tenant_hint(self) -> None:
        pid = uuid4()
        a = VerifiedAssertion(
            issuer="forze:jwt",
            subject=str(pid),
            issuer_tenant_hint="not-a-uuid",
        )
        ident = await JwtNativeUuidResolver().resolve(a)

        assert ident.principal_id == pid
        assert not hasattr(ident, "tenant_id")

    @pytest.mark.asyncio
    async def test_rejects_non_uuid_subject(self) -> None:
        a = VerifiedAssertion(issuer="firebase", subject="not-a-uuid")

        with pytest.raises(AuthenticationError):
            await JwtNativeUuidResolver().resolve(a)

# ....................... #


class TestDeterministicUuidResolver:
    @pytest.mark.asyncio
    async def test_same_pair_yields_same_principal(self) -> None:
        a = VerifiedAssertion(issuer="firebase", subject="user:42")
        ident_1 = await DeterministicUuidResolver().resolve(a)
        ident_2 = await DeterministicUuidResolver().resolve(a)

        assert ident_1.principal_id == ident_2.principal_id

    @pytest.mark.asyncio
    async def test_different_issuer_avoids_collision(self) -> None:
        a = VerifiedAssertion(issuer="firebase", subject="user:42")
        b = VerifiedAssertion(issuer="casdoor", subject="user:42")

        ident_a = await DeterministicUuidResolver().resolve(a)
        ident_b = await DeterministicUuidResolver().resolve(b)

        assert ident_a.principal_id != ident_b.principal_id

    @pytest.mark.asyncio
    async def test_issuer_tenant_hint_is_not_folded_into_identity(self) -> None:
        a = VerifiedAssertion(
            issuer="firebase",
            subject="x",
            issuer_tenant_hint="acme",
        )
        ident = await DeterministicUuidResolver().resolve(a)

        assert isinstance(ident.principal_id, UUID)
        assert not hasattr(ident, "tenant_id")

    def test_helper_matches_resolver(self) -> None:
        derived = derive_principal_id("firebase", "user:42")
        expected = deterministic_uuid4({"iss": "firebase", "sub": "user:42"})

        assert derived == expected


# ....................... #


class _StubPasswordVerifier:
    """In-memory stub returning a deterministic assertion."""

    async def verify_password(self, c: PasswordCredentials) -> VerifiedAssertion:
        return VerifiedAssertion(
            issuer="stub:password", subject="00000000-0000-0000-0000-000000000001"
        )


class _StubTokenVerifier:
    async def verify_token(self, c: AccessTokenCredentials) -> VerifiedAssertion:
        return VerifiedAssertion(
            issuer="stub:token", subject="00000000-0000-0000-0000-000000000002"
        )


class _StubScopedTokenVerifier:
    async def verify_token(self, c: AccessTokenCredentials) -> VerifiedAssertion:
        return VerifiedAssertion(
            issuer="stub:token",
            subject="00000000-0000-0000-0000-000000000002",
            issuer_tenant_hint="tenant-7",
        )


class _StubApiKeyVerifier:
    async def verify_api_key(self, c: ApiKeyCredentials) -> VerifiedAssertion:
        return VerifiedAssertion(
            issuer="stub:api_key", subject="00000000-0000-0000-0000-000000000003"
        )


class _CountingResolver:
    def __init__(self) -> None:
        self.calls: list[VerifiedAssertion] = []

    async def resolve(self, assertion: VerifiedAssertion) -> AuthnIdentity:
        self.calls.append(assertion)

        return AuthnIdentity(principal_id=UUID(assertion.subject))


# ....................... #


class TestAuthnOrchestrator:
    def _orch(
        self,
        methods: frozenset[str],
        *,
        password: object | None = None,
        token: object | None = None,
        api_key: object | None = None,
        resolver: object | None = None,
    ) -> AuthnOrchestrator:
        return AuthnOrchestrator(
            resolver=resolver or _CountingResolver(),  # type: ignore[arg-type]
            enabled_methods=methods,
            password_verifier=password,  # type: ignore[arg-type]
            token_verifier=token,  # type: ignore[arg-type]
            api_key_verifier=api_key,  # type: ignore[arg-type]
        )

    def test_post_init_requires_verifier_for_each_enabled_method(self) -> None:
        with pytest.raises(CoreError, match="TokenVerifierPort"):
            self._orch(frozenset({"token"}))

        with pytest.raises(CoreError, match="PasswordVerifierPort"):
            self._orch(frozenset({"password"}))

        with pytest.raises(CoreError, match="ApiKeyVerifierPort"):
            self._orch(frozenset({"api_key"}))

    @pytest.mark.asyncio
    async def test_disabled_method_raises_authentication_error(self) -> None:
        orch = self._orch(
            frozenset({"token"}),
            token=_StubTokenVerifier(),
        )

        with pytest.raises(AuthenticationError, match="not enabled"):
            await orch.authenticate_with_password(
                PasswordCredentials(login="x", password="y")
            )

    @pytest.mark.asyncio
    async def test_token_flow_round_trip(self) -> None:
        resolver = _CountingResolver()
        orch = self._orch(
            frozenset({"token"}),
            token=_StubTokenVerifier(),
            resolver=resolver,
        )

        result = await orch.authenticate_with_token(AccessTokenCredentials(token="t"))

        assert isinstance(result.identity, AuthnIdentity)
        assert result.identity.principal_id == UUID(
            "00000000-0000-0000-0000-000000000002"
        )
        assert result.issuer_tenant_hint is None
        assert len(resolver.calls) == 1
        assert resolver.calls[0].issuer == "stub:token"

    @pytest.mark.asyncio
    async def test_token_flow_preserves_issuer_tenant_hint_on_result(self) -> None:
        orch = self._orch(
            frozenset({"token"}),
            token=_StubScopedTokenVerifier(),
        )

        result = await orch.authenticate_with_token(AccessTokenCredentials(token="t"))

        assert result.issuer_tenant_hint == "tenant-7"

    @pytest.mark.asyncio
    async def test_multi_method_route_uses_distinct_verifiers(self) -> None:
        resolver = _CountingResolver()
        orch = self._orch(
            frozenset({"token", "password", "api_key"}),
            token=_StubTokenVerifier(),
            password=_StubPasswordVerifier(),
            api_key=_StubApiKeyVerifier(),
            resolver=resolver,
        )

        await orch.authenticate_with_token(AccessTokenCredentials(token="t"))
        await orch.authenticate_with_password(
            PasswordCredentials(login="u", password="p")
        )
        await orch.authenticate_with_api_key(ApiKeyCredentials(key="k"))

        issuers = [a.issuer for a in resolver.calls]
        assert issuers == ["stub:token", "stub:password", "stub:api_key"]
