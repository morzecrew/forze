"""Unit tests for verifier ports, resolver implementations, and the orchestrator.

These cover the new seams introduced by the strategic authn refactor:

* :class:`VerifiedAssertion` is the only thing that flows from a verifier to a resolver.
* Resolvers are independently swappable and can be unit-tested in isolation.
* :class:`AuthnOrchestrator` enforces ``enabled_methods`` regardless of which verifiers
  happen to be wired.
"""

from __future__ import annotations

from forze.base.exceptions import CoreException, exc
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
from forze.base.primitives import uuid4 as deterministic_uuid4
from unittest.mock import AsyncMock, MagicMock

from forze_identity.authn import (
    AuthnOrchestrator,
    DeterministicUuidResolver,
    ForzeJwtTokenVerifier,
    JwtNativeUuidResolver,
)
from forze_identity.authn.resolvers.deterministic_uuid import derive_principal_id
from forze_identity.authn.services import AccessTokenService, Hs256Signer

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


class TestForzeJwtSessionVerifier:
    @pytest.mark.asyncio
    async def test_active_session_passes(self) -> None:
        import secrets

        secret = secrets.token_bytes(32)
        svc = AccessTokenService(signer=Hs256Signer(secret=secret))
        pid = uuid4()
        sid = uuid4()
        token = await svc.issue_token(principal_id=pid, session_id=sid)

        session = MagicMock()
        session.principal_id = pid
        session.tenant_id = None
        session.revoked_at = None
        session.rotated_at = None
        session_qry = MagicMock()
        session_qry.find = AsyncMock(return_value=session)

        verifier = ForzeJwtTokenVerifier(access_svc=svc, session_qry=session_qry)
        assertion = await verifier.verify_token(AccessTokenCredentials(token=token))

        assert assertion.subject == str(pid)
        session_qry.find.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_missing_sid_rejected(self) -> None:
        import secrets

        secret = secrets.token_bytes(32)
        svc = AccessTokenService(signer=Hs256Signer(secret=secret))
        token = await svc.issue_token(principal_id=uuid4())
        session_qry = MagicMock()

        verifier = ForzeJwtTokenVerifier(access_svc=svc, session_qry=session_qry)

        with pytest.raises(CoreException) as ei:
            await verifier.verify_token(AccessTokenCredentials(token=token))
        assert ei.value.code == "invalid_access_token"
        session_qry.find.assert_not_called()

    @pytest.mark.asyncio
    async def test_revoked_session_rejected(self) -> None:
        import secrets

        secret = secrets.token_bytes(32)
        svc = AccessTokenService(signer=Hs256Signer(secret=secret))
        sid = uuid4()
        token = await svc.issue_token(principal_id=uuid4(), session_id=sid)

        session = MagicMock()
        session.revoked_at = datetime.now(tz=UTC)
        session.rotated_at = None
        session_qry = MagicMock()
        session_qry.find = AsyncMock(return_value=session)

        verifier = ForzeJwtTokenVerifier(access_svc=svc, session_qry=session_qry)

        with pytest.raises(CoreException) as ei:
            await verifier.verify_token(AccessTokenCredentials(token=token))
        assert ei.value.code == "session_revoked"

    @pytest.mark.asyncio
    async def test_rotated_session_rejected(self) -> None:
        import secrets

        secret = secrets.token_bytes(32)
        svc = AccessTokenService(signer=Hs256Signer(secret=secret))
        sid = uuid4()
        token = await svc.issue_token(principal_id=uuid4(), session_id=sid)

        session = MagicMock()
        session.revoked_at = None
        session.rotated_at = datetime.now(tz=UTC)
        session_qry = MagicMock()
        session_qry.find = AsyncMock(return_value=session)

        verifier = ForzeJwtTokenVerifier(access_svc=svc, session_qry=session_qry)

        with pytest.raises(CoreException) as ei:
            await verifier.verify_token(AccessTokenCredentials(token=token))
        assert ei.value.code == "session_revoked"

    @pytest.mark.asyncio
    async def test_session_subject_mismatch_rejected(self) -> None:
        import secrets

        secret = secrets.token_bytes(32)
        svc = AccessTokenService(signer=Hs256Signer(secret=secret))
        token_pid = uuid4()
        session_pid = uuid4()
        sid = uuid4()
        token = await svc.issue_token(principal_id=token_pid, session_id=sid)

        session = MagicMock()
        session.principal_id = session_pid
        session.tenant_id = None
        session.revoked_at = None
        session.rotated_at = None
        session_qry = MagicMock()
        session_qry.find = AsyncMock(return_value=session)

        verifier = ForzeJwtTokenVerifier(access_svc=svc, session_qry=session_qry)

        with pytest.raises(CoreException) as ei:
            await verifier.verify_token(AccessTokenCredentials(token=token))
        assert ei.value.code == "session_subject_mismatch"

    @pytest.mark.asyncio
    async def test_session_tenant_mismatch_rejected(self) -> None:
        import secrets

        secret = secrets.token_bytes(32)
        svc = AccessTokenService(signer=Hs256Signer(secret=secret))
        pid = uuid4()
        sid = uuid4()
        token_tid = uuid4()
        session_tid = uuid4()
        token = await svc.issue_token(
            principal_id=pid,
            tenant_id=token_tid,
            session_id=sid,
        )

        session = MagicMock()
        session.principal_id = pid
        session.tenant_id = session_tid
        session.revoked_at = None
        session.rotated_at = None
        session_qry = MagicMock()
        session_qry.find = AsyncMock(return_value=session)

        verifier = ForzeJwtTokenVerifier(access_svc=svc, session_qry=session_qry)

        with pytest.raises(CoreException) as ei:
            await verifier.verify_token(AccessTokenCredentials(token=token))
        assert ei.value.code == "session_tenant_mismatch"

    @pytest.mark.asyncio
    async def test_session_tenant_ok_when_token_omits_tid(self) -> None:
        import secrets

        secret = secrets.token_bytes(32)
        svc = AccessTokenService(signer=Hs256Signer(secret=secret))
        pid = uuid4()
        sid = uuid4()
        session_tid = uuid4()
        token = await svc.issue_token(principal_id=pid, session_id=sid)

        session = MagicMock()
        session.principal_id = pid
        session.tenant_id = session_tid
        session.revoked_at = None
        session.rotated_at = None
        session_qry = MagicMock()
        session_qry.find = AsyncMock(return_value=session)

        verifier = ForzeJwtTokenVerifier(access_svc=svc, session_qry=session_qry)
        assertion = await verifier.verify_token(AccessTokenCredentials(token=token))

        assert assertion.subject == str(pid)
        assert assertion.issuer_tenant_hint is None


# ....................... #
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

        with pytest.raises(CoreException):
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

class _StubDelegatedTokenVerifier:
    """Token verifier whose assertion carries an RFC 8693 ``act`` (actor) claim."""

    def __init__(self, subject: str, claims: dict[str, object]) -> None:
        self._subject = subject
        self._claims = claims

    async def verify_token(self, c: AccessTokenCredentials) -> VerifiedAssertion:
        _ = c
        return VerifiedAssertion(
            issuer="stub:token", subject=self._subject, claims=self._claims
        )

# ....................... #

def _noop_eligibility() -> MagicMock:
    eligibility = MagicMock()
    eligibility.require_authentication_allowed = AsyncMock()
    return eligibility


class TestAuthnOrchestrator:
    def _orch(
        self,
        methods: frozenset[str],
        *,
        password: object | None = None,
        token: object | None = None,
        api_key: object | None = None,
        resolver: object | None = None,
        eligibility: object | None = None,
    ) -> AuthnOrchestrator:
        return AuthnOrchestrator(
            resolver=resolver or _CountingResolver(),  # type: ignore[arg-type]
            eligibility=eligibility or _noop_eligibility(),  # type: ignore[arg-type]
            enabled_methods=methods,
            password_verifier=password,  # type: ignore[arg-type]
            token_verifier=token,  # type: ignore[arg-type]
            api_key_verifier=api_key,  # type: ignore[arg-type]
        )

    def test_post_init_requires_verifier_for_each_enabled_method(self) -> None:
        with pytest.raises(CoreException, match="TokenVerifierPort"):
            self._orch(frozenset({"token"}))

        with pytest.raises(CoreException, match="PasswordVerifierPort"):
            self._orch(frozenset({"password"}))

        with pytest.raises(CoreException, match="ApiKeyVerifierPort"):
            self._orch(frozenset({"api_key"}))

    @pytest.mark.asyncio
    async def test_disabled_method_raises_authentication_error(self) -> None:
        orch = self._orch(
            frozenset({"token"}),
            token=_StubTokenVerifier(),
        )

        with pytest.raises(CoreException, match="not enabled"):
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


# ....................... #


class TestDelegationActorClaim:
    """The orchestrator reads an RFC 8693 ``act`` claim into ``AuthnIdentity.actor``."""

    USER = "00000000-0000-0000-0000-000000000002"
    AGENT = "00000000-0000-0000-0000-0000000000aa"
    SYSTEM = "00000000-0000-0000-0000-0000000000bb"

    def _orch(
        self,
        claims: dict[str, object],
        *,
        actor_claim: str | None = "act",
        eligibility: object | None = None,
    ) -> AuthnOrchestrator:
        return AuthnOrchestrator(
            resolver=_CountingResolver(),
            eligibility=eligibility or _noop_eligibility(),  # type: ignore[arg-type]
            enabled_methods=frozenset({"token"}),
            token_verifier=_StubDelegatedTokenVerifier(self.USER, claims),  # type: ignore[arg-type]
            actor_claim=actor_claim,
        )

    @pytest.mark.asyncio
    async def test_actor_attached_from_act_claim(self) -> None:
        orch = self._orch({"act": {"sub": self.AGENT}})

        result = await orch.authenticate_with_token(AccessTokenCredentials(token="t"))

        assert result.identity.principal_id == UUID(self.USER)
        assert result.identity.is_delegated is True
        assert result.identity.actor is not None
        assert result.identity.actor.principal_id == UUID(self.AGENT)
        assert result.identity.actor.actor is None

    @pytest.mark.asyncio
    async def test_nested_act_builds_delegation_chain(self) -> None:
        orch = self._orch({"act": {"sub": self.AGENT, "act": {"sub": self.SYSTEM}}})

        result = await orch.authenticate_with_token(AccessTokenCredentials(token="t"))

        actor = result.identity.actor
        assert actor is not None
        assert actor.principal_id == UUID(self.AGENT)
        assert actor.actor is not None
        assert actor.actor.principal_id == UUID(self.SYSTEM)

    @pytest.mark.asyncio
    async def test_act_claim_ignored_when_actor_claim_unset(self) -> None:
        orch = self._orch({"act": {"sub": self.AGENT}}, actor_claim=None)

        result = await orch.authenticate_with_token(AccessTokenCredentials(token="t"))

        assert result.identity.actor is None
        assert result.identity.is_delegated is False

    @pytest.mark.asyncio
    async def test_missing_actor_sub_raises(self) -> None:
        orch = self._orch({"act": {"not_sub": "x"}})

        with pytest.raises(CoreException) as exc_info:
            await orch.authenticate_with_token(AccessTokenCredentials(token="t"))
        assert exc_info.value.code == "invalid_actor_claim"

    @pytest.mark.asyncio
    async def test_eligibility_checked_for_subject_and_actor(self) -> None:
        eligibility = _noop_eligibility()
        orch = self._orch({"act": {"sub": self.AGENT}}, eligibility=eligibility)

        await orch.authenticate_with_token(AccessTokenCredentials(token="t"))

        # Once for the user (subject), once for the agent (actor).
        assert eligibility.require_authentication_allowed.await_count == 2

    @pytest.mark.asyncio
    async def test_deeply_nested_actor_chain_is_rejected(self) -> None:
        # A chain deeper than the cap must be refused, not recursed unbounded.
        root: dict[str, object] = {"sub": str(uuid4())}
        node = root
        for _ in range(12):
            nxt: dict[str, object] = {"sub": str(uuid4())}
            node["act"] = nxt
            node = nxt

        orch = self._orch({"act": root})

        with pytest.raises(CoreException) as exc_info:
            await orch.authenticate_with_token(AccessTokenCredentials(token="t"))
        assert exc_info.value.code == "actor_chain_too_deep"
