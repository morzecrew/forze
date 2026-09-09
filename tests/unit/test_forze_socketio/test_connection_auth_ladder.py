"""The shipped Socket.IO connection resolver — the WebSocket ladder, other handshake.

# covers: forze_socketio.auth (build_socketio_connection_resolver: the WSGI environ
#         read as cookies/headers/query, the reauth-shaped connect, cookie attestation)

One ladder serves both transports, so what is proven here is the translation: a
python-socketio ``environ`` produces the same decisions the raw-WebSocket upgrade
does, including the ones a hand-written second copy would get subtly wrong.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnResult,
    AuthnSpec,
)
from forze.application.contracts.authn import ClientIdentity as _ClientIdentity
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.testing import context_from_deps
from forze_socketio import SocketIOConnect, build_socketio_connection_resolver, socketio_handshake

pytestmark = pytest.mark.unit

# ----------------------- #

_SPEC = AuthnSpec(name="realtime", enabled_methods=frozenset({"token"}))
_PRINCIPAL = UUID("11111111-1111-1111-1111-111111111111")
_EXPIRY = datetime(2027, 1, 1, tzinfo=UTC)
_COOKIE = "forze_access"

_GOOD = "good-token"
_REVOKED = "revoked-token"


class _AuthnPort:
    def __init__(self) -> None:
        self.seen: list[str] = []

    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(self, credentials: AccessTokenCredentials) -> AuthnResult:
        self.seen.append(credentials.token)

        if credentials.token != _GOOD:
            raise exc.authentication("Invalid access token", code="invalid_access_token")

        return AuthnResult(
            identity=AuthnIdentity(principal_id=_PRINCIPAL),
            expires_at=_EXPIRY,
        )

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult | None:
        return None


_PORT = _AuthnPort()


class _AuthnFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _AuthnPort:
        _ = ctx, spec

        return _PORT


def _resolver(**kwargs: Any) -> Any:
    deps = Deps.plain({AuthnDepKey: _AuthnFactory()})

    return build_socketio_connection_resolver(
        ctx_dep=lambda: context_from_deps(deps),
        authn_spec=_SPEC,
        **kwargs,
    )


def _connect(
    *,
    environ: dict[str, Any] | None = None,
    auth: Any = None,
) -> SocketIOConnect:
    return SocketIOConnect(sid="sid-1", namespace="/", environ=environ or {}, auth=auth)


# ----------------------- #


class TestTheEnvironIsReadLikeAnUpgradeRequest:
    def test_the_cookie_header_becomes_cookies(self) -> None:
        handshake = socketio_handshake(
            _connect(environ={"HTTP_COOKIE": f"{_COOKIE}={_GOOD}; other=x"})
        )

        assert handshake.cookies == {_COOKIE: _GOOD, "other": "x"}

    def test_a_malformed_cookie_header_reads_as_no_cookie(self) -> None:
        # Client input: it must mean "presented nothing" and let the ladder move on,
        # never a 500 out of the connect handler.
        handshake = socketio_handshake(_connect(environ={"HTTP_COOKIE": "=;;;"}))

        assert handshake.cookies == {}

    def test_wsgi_header_names_become_header_names(self) -> None:
        handshake = socketio_handshake(
            _connect(environ={"HTTP_AUTHORIZATION": f"Bearer {_GOOD}", "REQUEST_METHOD": "GET"})
        )

        assert handshake.header("Authorization") == f"Bearer {_GOOD}"

    def test_the_query_string_is_parsed(self) -> None:
        handshake = socketio_handshake(_connect(environ={"QUERY_STRING": "token=t&device_id=d"}))

        assert handshake.query == {"token": "t", "device_id": "d"}

    def test_a_non_mapping_auth_payload_is_ignored(self) -> None:
        # python-socketio hands through whatever the client sent — a string, a list.
        assert socketio_handshake(_connect(auth="just-a-string")).auth is None


class TestTheSameLadderDecisions:
    @pytest.mark.asyncio
    async def test_the_header_authenticates_and_carries_the_expiry(self) -> None:
        connection = await _resolver()(
            _connect(environ={"HTTP_AUTHORIZATION": f"Bearer {_GOOD}"})
        )

        assert connection is not None
        assert connection.authn.principal_id == _PRINCIPAL
        assert connection.expires_at == _EXPIRY

    @pytest.mark.asyncio
    async def test_the_query_source_is_off_by_default(self) -> None:
        connection = await _resolver()(_connect(environ={"QUERY_STRING": f"token={_GOOD}"}))

        assert connection is None

    @pytest.mark.asyncio
    async def test_the_cookie_outranks_the_header_and_does_not_fall_through(self) -> None:
        resolve = _resolver(cookie_name=_COOKIE, origin_allowlist_attested=True)

        with pytest.raises(CoreException) as caught:
            await resolve(
                _connect(
                    environ={
                        "HTTP_COOKIE": f"{_COOKIE}={_REVOKED}",
                        "HTTP_AUTHORIZATION": f"Bearer {_GOOD}",
                    }
                )
            )

        assert caught.value.kind is ExceptionKind.AUTHENTICATION

    @pytest.mark.asyncio
    async def test_a_reauth_shaped_connect_is_answered_by_the_payload(self) -> None:
        # The connection layer rebuilds a connect with an empty environ for reauth, so
        # the payload rung is the only one that can answer it.
        connection = await _resolver()(_connect(auth={"token": _GOOD, "device_id": "phone-7"}))

        assert connection is not None
        assert connection.client == _ClientIdentity(device_id="phone-7")

    @pytest.mark.asyncio
    async def test_an_anonymous_handshake_resolves_none(self) -> None:
        assert await _resolver()(_connect()) is None


class TestCookieAttestation:
    def test_cookie_mode_without_it_is_refused_at_construction(self) -> None:
        with pytest.raises(CoreException) as caught:
            _resolver(cookie_name=_COOKIE)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "realtime_cookie_origin_unattested"
        assert "cors_allowed_origins" in str(caught.value)

    def test_the_header_only_default_needs_no_attestation(self) -> None:
        assert _resolver()
