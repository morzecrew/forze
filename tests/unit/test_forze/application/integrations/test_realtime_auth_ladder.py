"""The realtime credential ladder — the one both transports authenticate through.

# covers: forze.application.integrations.realtime.auth (source order, no-fallthrough
#         refusal, the off-by-default query source, expiry and tenant mapping)
# covers: forze.application.integrations.authn.orchestrator (the verified assertion's
#         expiry reaching AuthnResult, which is what lets a connection outlive nothing)

The ladder is the whole of the shipped resolver that is not transport plumbing, so the
order, the refusals and the mapping are pinned here once rather than twice per transport.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    ApiKeyCredentials,
    AuthnIdentity,
    AuthnResult,
    ClientIdentity,
    PasswordCredentials,
    VerifiedAssertion,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.authn import AuthnOrchestrator
from forze.application.integrations.realtime import (
    RealtimeCredentialSources,
    RealtimeHandshake,
    client_identity,
    present_credential,
    resolve_realtime_identity,
)
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit

# ----------------------- #

_PRINCIPAL = uuid4()
_TENANT = uuid4()
_BOUND_TENANT = uuid4()
_EXPIRY = datetime(2027, 1, 1, tzinfo=UTC)


class _Authn:
    """A token plane that answers with the token itself, so a test can see which won."""

    def __init__(
        self,
        *,
        rejects: frozenset[str] = frozenset(),
        expires_at: datetime | None = _EXPIRY,
        tenant_hint: str | None = None,
    ) -> None:
        self.rejects = rejects
        self.expires_at = expires_at
        self.tenant_hint = tenant_hint
        self.seen: list[AccessTokenCredentials] = []

    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(self, credentials: AccessTokenCredentials) -> AuthnResult:
        self.seen.append(credentials)

        if credentials.token in self.rejects:
            from forze.base.exceptions import exc

            raise exc.authentication("Invalid access token", code="invalid_access_token")

        return AuthnResult(
            identity=AuthnIdentity(principal_id=_PRINCIPAL),
            issuer_tenant_hint=self.tenant_hint,
            expires_at=self.expires_at,
        )

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult | None:
        return None


class _Tenants:
    def __init__(self, answer: TenantIdentity | None) -> None:
        self.answer = answer
        self.requested: list[UUID | None] = []

    async def resolve_from_principal(
        self,
        principal_id: UUID,
        *,
        requested_tenant_id: UUID | None = None,
    ) -> TenantIdentity | None:
        _ = principal_id
        self.requested.append(requested_tenant_id)

        return self.answer


def _handshake(**kwargs: Any) -> RealtimeHandshake:
    return RealtimeHandshake(**kwargs)


async def _resolve(
    sources: RealtimeCredentialSources,
    handshake: RealtimeHandshake,
    *,
    authn: _Authn | None = None,
    tenants: _Tenants | None = None,
) -> Any:
    return await resolve_realtime_identity(
        authn=authn or _Authn(),  # pyright: ignore[reportArgumentType]
        sources=sources,
        handshake=handshake,
        tenants=tenants,  # pyright: ignore[reportArgumentType]
    )


# ----------------------- #


class TestSourceOrder:
    def test_the_payload_answers_before_any_request_source(self) -> None:
        presented = present_credential(
            RealtimeCredentialSources(cookie_name="c", query_param="token"),
            _handshake(
                cookies={"c": "cookie-token"},
                headers={"Authorization": "Bearer header-token"},
                query={"token": "query-token"},
                auth={"token": "payload-token"},
            ),
        )

        assert presented is not None
        assert (presented.token, presented.source) == ("payload-token", "payload")

    def test_the_cookie_beats_the_header_and_the_header_beats_the_query(self) -> None:
        sources = RealtimeCredentialSources(cookie_name="c", query_param="token")
        full = _handshake(
            cookies={"c": "cookie-token"},
            headers={"Authorization": "Bearer header-token"},
            query={"token": "query-token"},
        )
        no_cookie = _handshake(
            headers={"Authorization": "Bearer header-token"},
            query={"token": "query-token"},
        )

        assert present_credential(sources, full) is not None
        assert present_credential(sources, full).source == "cookie"  # pyright: ignore[reportOptionalMemberAccess]
        assert present_credential(sources, no_cookie).source == "header"  # pyright: ignore[reportOptionalMemberAccess]

    def test_a_disabled_source_is_not_read_even_when_it_carries_a_token(self) -> None:
        # The whole point of `cookie_name=None`: a cookie the deployment never opted
        # into must not authenticate anything.
        presented = present_credential(
            RealtimeCredentialSources(),
            _handshake(cookies={"forze_access": "cookie-token"}),
        )

        assert presented is None

    def test_a_disabled_header_source_leaves_the_query_to_answer(self) -> None:
        presented = present_credential(
            RealtimeCredentialSources(header_name=None, query_param="token"),
            _handshake(
                headers={"Authorization": "Bearer header-token"},
                query={"token": "query-token"},
            ),
        )

        assert presented is not None
        assert (presented.token, presented.source) == ("query-token", "query")

    def test_the_query_source_is_off_by_default(self) -> None:
        presented = present_credential(
            RealtimeCredentialSources(),
            _handshake(query={"token": "query-token"}),
        )

        assert presented is None

    def test_the_header_is_read_case_insensitively_and_a_bare_token_is_accepted(self) -> None:
        with_scheme = present_credential(
            RealtimeCredentialSources(),
            _handshake(headers={"authorization": "Bearer tok"}),
        )
        bare = present_credential(
            RealtimeCredentialSources(),
            _handshake(headers={"AUTHORIZATION": "tok"}),
        )

        assert with_scheme is not None and bare is not None
        assert (with_scheme.token, with_scheme.scheme) == ("tok", "Bearer")
        assert (bare.token, bare.scheme) == ("tok", "Bearer")

    def test_a_blank_source_is_not_a_presented_credential(self) -> None:
        presented = present_credential(
            RealtimeCredentialSources(cookie_name="c"),
            _handshake(cookies={"c": "   "}, headers={"Authorization": "Bearer tok"}),
        )

        assert presented is not None
        assert presented.source == "header"

    def test_a_header_carrying_only_a_scheme_presents_nothing(self) -> None:
        # "Authorization: Bearer" with no token would otherwise send the literal word
        # "Bearer" to the verifier — refused, and under the no-fallthrough rule that
        # refusal would take the rest of the ladder down with it.
        presented = present_credential(
            RealtimeCredentialSources(query_param="token"),
            _handshake(headers={"Authorization": "Bearer"}, query={"token": "query-token"}),
        )

        assert presented is not None
        assert (presented.token, presented.source) == ("query-token", "query")

    @pytest.mark.parametrize("header", ["Bearer", "bearer", "Bearer   ", "  Bearer"])
    def test_a_scheme_and_nothing_else_presents_nothing(self, header: str) -> None:
        presented = present_credential(
            RealtimeCredentialSources(query_param="token"),
            _handshake(headers={"Authorization": header}, query={"token": "query-token"}),
        )

        assert presented is not None
        assert presented.source == "query"

    def test_a_payload_that_carries_no_token_falls_through_to_the_request(self) -> None:
        # A Socket.IO connect sends `{"protocol": 1}` with the credential in a cookie;
        # a reauth sends the token. Both are the same payload field, so a payload
        # without a token must not shadow the request's own sources.
        presented = present_credential(
            RealtimeCredentialSources(cookie_name="c"),
            _handshake(cookies={"c": "cookie-token"}, auth={"protocol": 1}),
        )

        assert presented is not None
        assert presented.source == "cookie"

    def test_a_blank_header_falls_through_to_the_query(self) -> None:
        presented = present_credential(
            RealtimeCredentialSources(query_param="token"),
            _handshake(headers={"Authorization": "   "}, query={"token": "query-token"}),
        )

        assert presented is not None
        assert presented.source == "query"

    def test_a_blank_query_parameter_presents_nothing(self) -> None:
        presented = present_credential(
            RealtimeCredentialSources(query_param="token"),
            _handshake(query={"token": "  "}),
        )

        assert presented is None

    def test_a_ladder_with_every_request_source_disabled_says_so(self) -> None:
        # Payload-only is a real Socket.IO wiring and an impossible WebSocket one, so
        # the ladder reports the shape and each transport decides what it means.
        payload_only = RealtimeCredentialSources(
            cookie_name=None, header_name=None, query_param=None
        )

        assert not payload_only.reads_the_request
        assert RealtimeCredentialSources().reads_the_request

    @pytest.mark.asyncio
    async def test_a_payload_only_ladder_authenticates_from_the_payload(self) -> None:
        identity = await _resolve(
            RealtimeCredentialSources(cookie_name=None, header_name=None, query_param=None),
            _handshake(headers={"Authorization": "Bearer ignored"}, auth={"token": "tok"}),
        )

        assert identity is not None
        assert identity.authn.principal_id == _PRINCIPAL


class TestNoFallthrough:
    @pytest.mark.asyncio
    async def test_an_invalid_cookie_refuses_instead_of_trying_the_header(self) -> None:
        # The downgrade this forbids: a revoked cookie silently retried as whatever
        # the next source carries — a different principal, on the same connection.
        authn = _Authn(rejects=frozenset({"revoked"}))

        with pytest.raises(CoreException) as caught:
            await _resolve(
                RealtimeCredentialSources(cookie_name="c"),
                _handshake(
                    cookies={"c": "revoked"},
                    headers={"Authorization": "Bearer still-good"},
                ),
                authn=authn,
            )

        assert caught.value.kind is ExceptionKind.AUTHENTICATION
        assert [c.token for c in authn.seen] == ["revoked"]

    @pytest.mark.asyncio
    async def test_a_handshake_with_no_credential_resolves_anonymous(self) -> None:
        assert await _resolve(RealtimeCredentialSources(), _handshake()) is None


class TestMapping:
    @pytest.mark.asyncio
    async def test_the_credentials_expiry_becomes_the_connections(self) -> None:
        identity = await _resolve(
            RealtimeCredentialSources(),
            _handshake(headers={"Authorization": "Bearer tok"}),
        )

        assert identity is not None
        assert identity.expires_at == _EXPIRY
        assert identity.expires_at.tzinfo is not None

    @pytest.mark.asyncio
    async def test_a_naive_expiry_is_refused_rather_than_assumed_utc(self) -> None:
        # Guessing UTC would enforce expiry at the wrong instant; the connection value
        # objects refuse a naive instant anyway, and would do it far from the cause.
        authn = _Authn(expires_at=datetime(2027, 1, 1))

        with pytest.raises(CoreException) as caught:
            await _resolve(
                RealtimeCredentialSources(),
                _handshake(headers={"Authorization": "Bearer tok"}),
                authn=authn,
            )

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "realtime_auth_expiry_naive"

    @pytest.mark.asyncio
    async def test_a_credential_without_an_expiry_never_expires(self) -> None:
        identity = await _resolve(
            RealtimeCredentialSources(),
            _handshake(headers={"Authorization": "Bearer tok"}),
            authn=_Authn(expires_at=None),
        )

        assert identity is not None
        assert identity.expires_at is None

    @pytest.mark.asyncio
    async def test_the_issuer_hint_is_the_tenant_when_nothing_resolves_one(self) -> None:
        identity = await _resolve(
            RealtimeCredentialSources(),
            _handshake(headers={"Authorization": "Bearer tok"}),
            authn=_Authn(tenant_hint=str(_TENANT)),
        )

        assert identity is not None
        assert identity.tenant == _TENANT

    @pytest.mark.asyncio
    async def test_the_resolvers_binding_outranks_the_issuer_hint(self) -> None:
        tenants = _Tenants(TenantIdentity(tenant_id=_BOUND_TENANT))

        identity = await _resolve(
            RealtimeCredentialSources(),
            _handshake(headers={"Authorization": "Bearer tok"}),
            authn=_Authn(tenant_hint=str(_TENANT)),
            tenants=tenants,
        )

        assert identity is not None
        assert identity.tenant == _BOUND_TENANT
        assert tenants.requested == [_TENANT]

    @pytest.mark.asyncio
    async def test_a_resolver_with_no_binding_leaves_the_verified_hint_standing(self) -> None:
        identity = await _resolve(
            RealtimeCredentialSources(),
            _handshake(headers={"Authorization": "Bearer tok"}),
            authn=_Authn(tenant_hint=str(_TENANT)),
            tenants=_Tenants(None),
        )

        assert identity is not None
        assert identity.tenant == _TENANT

    @pytest.mark.asyncio
    async def test_an_unparseable_tenant_hint_is_no_tenant(self) -> None:
        identity = await _resolve(
            RealtimeCredentialSources(),
            _handshake(headers={"Authorization": "Bearer tok"}),
            authn=_Authn(tenant_hint="not-a-uuid"),
        )

        assert identity is not None
        assert identity.tenant is None


class TestClientIdentity:
    def test_the_query_names_the_device(self) -> None:
        assert client_identity(_handshake(query={"device_id": "dev-1"})) == ClientIdentity(
            device_id="dev-1"
        )

    def test_the_payload_outranks_the_query(self) -> None:
        # A reauth carries the payload and no query string; connect may carry both.
        resolved = client_identity(
            _handshake(query={"device_id": "from-query"}, auth={"device_id": "from-payload"})
        )

        assert resolved == ClientIdentity(device_id="from-payload")

    def test_a_handshake_that_names_neither_has_no_client(self) -> None:
        assert client_identity(_handshake(query={"device_id": "  "})) is None

    def test_the_keys_are_configurable(self) -> None:
        resolved = client_identity(
            _handshake(query={"dev": "d", "sess": "s"}),
            device_id_key="dev",
            session_id_key="sess",
        )

        assert resolved == ClientIdentity(device_id="d", session_id="s")

    @pytest.mark.asyncio
    async def test_the_resolved_identity_carries_it(self) -> None:
        identity = await _resolve(
            RealtimeCredentialSources(),
            _handshake(headers={"Authorization": "Bearer tok"}, query={"device_id": "dev-1"}),
        )

        assert identity is not None
        assert identity.client == ClientIdentity(device_id="dev-1")


class TestTokensStayOutOfDiagnostics:
    def test_the_presented_credential_does_not_repr_its_token(self) -> None:
        presented = present_credential(
            RealtimeCredentialSources(),
            _handshake(headers={"Authorization": "Bearer super-secret"}),
        )

        assert presented is not None
        assert "super-secret" not in repr(presented)

    @pytest.mark.asyncio
    async def test_a_refusal_names_the_source_and_not_the_token(self) -> None:
        with pytest.raises(CoreException) as caught:
            await _resolve(
                RealtimeCredentialSources(cookie_name="c"),
                _handshake(cookies={"c": "super-secret"}),
                authn=_Authn(expires_at=datetime(2027, 1, 1)),
            )

        assert "cookie" in str(caught.value)
        assert "super-secret" not in str(caught.value)


# ----------------------- #


class _ExpiringVerifier:
    """A verifier that asserts an expiry, as every real token verifier does."""

    def __init__(self, expires_at: datetime | None) -> None:
        self.expires_at = expires_at

    async def verify_token(self, credentials: AccessTokenCredentials) -> VerifiedAssertion:
        _ = credentials

        return VerifiedAssertion(
            issuer="test",
            subject=str(_PRINCIPAL),
            expires_at=self.expires_at,
        )

    async def verify_api_key(self, credentials: ApiKeyCredentials) -> VerifiedAssertion:
        _ = credentials

        return VerifiedAssertion(
            issuer="test",
            subject=str(_PRINCIPAL),
            expires_at=self.expires_at,
        )

    async def verify_password(self, credentials: PasswordCredentials) -> VerifiedAssertion:
        _ = credentials

        return VerifiedAssertion(
            issuer="test",
            subject=str(_PRINCIPAL),
            expires_at=self.expires_at,
        )


class _Resolver:
    async def resolve(self, assertion: VerifiedAssertion) -> AuthnIdentity:
        return AuthnIdentity(principal_id=UUID(assertion.subject))


class _Eligibility:
    async def require_authentication_allowed(self, principal_id: UUID) -> None:
        _ = principal_id


def _orchestrator(expires_at: datetime | None) -> AuthnOrchestrator:
    verifier = _ExpiringVerifier(expires_at)

    return AuthnOrchestrator(
        resolver=_Resolver(),
        eligibility=_Eligibility(),
        enabled_methods=frozenset({"token", "api_key", "password"}),
        token_verifier=verifier,
        api_key_verifier=verifier,
        password_verifier=verifier,
    )


class TestTheAssertionsExpiryReachesTheResult:
    """Without this the ladder could only get an expiry by decoding the token itself —
    behind the verifier's back, in a format only some verifiers speak."""

    @pytest.mark.asyncio
    async def test_the_token_path_carries_it(self) -> None:
        result = await _orchestrator(_EXPIRY).authenticate_with_token(
            AccessTokenCredentials(token="tok")
        )

        assert result.expires_at == _EXPIRY

    @pytest.mark.asyncio
    async def test_the_api_key_path_carries_it(self) -> None:
        result = await _orchestrator(_EXPIRY).authenticate_with_api_key(ApiKeyCredentials(key="k"))

        assert result.expires_at == _EXPIRY

    @pytest.mark.asyncio
    async def test_a_verifier_that_asserts_no_expiry_yields_none(self) -> None:
        result = await _orchestrator(None).authenticate_with_token(
            AccessTokenCredentials(token="tok")
        )

        assert result.expires_at is None

    @pytest.mark.asyncio
    async def test_the_expiry_survives_the_whole_ladder(self) -> None:
        identity = await resolve_realtime_identity(
            authn=_orchestrator(_EXPIRY),  # pyright: ignore[reportArgumentType]
            sources=RealtimeCredentialSources(),
            handshake=_handshake(headers={"Authorization": "Bearer tok"}),
        )

        assert identity is not None
        assert identity.expires_at == _EXPIRY
        assert identity.authn.principal_id == _PRINCIPAL


class TestExpiryIsNotInvented:
    @pytest.mark.asyncio
    async def test_an_expiry_in_the_past_is_passed_through_untouched(self) -> None:
        # The ladder does not judge freshness: the verifier already refused an expired
        # token, and the transports enforce the instant continuously.
        past = datetime.now(tz=UTC) - timedelta(hours=1)

        identity = await _resolve(
            RealtimeCredentialSources(),
            _handshake(headers={"Authorization": "Bearer tok"}),
            authn=_Authn(expires_at=past),
        )

        assert identity is not None
        assert identity.expires_at == past
