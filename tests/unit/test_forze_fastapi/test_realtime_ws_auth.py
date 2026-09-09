"""The shipped WebSocket connection resolver, driven through the real route.

# covers: forze_fastapi.realtime.auth (build_ws_connection_resolver: the credential
#         ladder on a live upgrade, the reauth path, the cookie/Origin construction
#         rule, and what a refusal is allowed to say)

The ladder itself is pinned in the transport-neutral suite; what is proven here is
that a real upgrade request reaches it — cookies, headers and query as starlette
presents them — and that what it resolves is what the route then enforces.
"""

from __future__ import annotations

import json
import time
from inspect import isawaitable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
import structlog
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnResult,
    AuthnSpec,
)
from forze.application.contracts.tenancy import TenantIdentity, TenantResolverDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.testing import context_from_deps
from forze.application.integrations.realtime import (
    InMemoryMailboxCursors,
    InMemoryRealtimeMailbox,
)
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_fastapi.realtime import attach_realtime_ws_route, build_ws_connection_resolver

pytestmark = pytest.mark.unit

# ----------------------- #

_SPEC = AuthnSpec(name="realtime", enabled_methods=frozenset({"token"}))
_PRINCIPAL = UUID("11111111-1111-1111-1111-111111111111")
_OTHER = UUID("22222222-2222-2222-2222-222222222222")
_COOKIE = "forze_access"

_GOOD = "good-token"
_OTHER_PRINCIPAL_TOKEN = "other-token"
_REVOKED = "revoked-token"
_SHORT_LIVED = "short-lived-token"


class _AuthnPort:
    """The wired token plane: one valid token per principal, one revoked, one expiring."""

    async def authenticate_with_password(self, credentials: object) -> AuthnResult | None:
        return None

    async def authenticate_with_token(self, credentials: AccessTokenCredentials) -> AuthnResult:
        token = credentials.token

        if token == _REVOKED:
            raise exc.authentication("Invalid access token", code="invalid_access_token")

        if token == _SHORT_LIVED:
            return AuthnResult(
                identity=AuthnIdentity(principal_id=_PRINCIPAL),
                expires_at=datetime.now(tz=UTC) + timedelta(milliseconds=400),
            )

        if token == _OTHER_PRINCIPAL_TOKEN:
            return AuthnResult(identity=AuthnIdentity(principal_id=_OTHER))

        if token != _GOOD:
            raise exc.authentication("Unknown access token", code="invalid_access_token")

        return AuthnResult(
            identity=AuthnIdentity(principal_id=_PRINCIPAL),
            expires_at=datetime(2027, 1, 1, tzinfo=UTC),
        )

    async def authenticate_with_api_key(self, credentials: object) -> AuthnResult | None:
        return None


class _AuthnFactory:
    def __call__(self, ctx: ExecutionContext, spec: AuthnSpec) -> _AuthnPort:
        _ = ctx, spec

        return _AuthnPort()


async def _allow_all(
    _ctx: ExecutionContext,
    _principal: str,
    _tenant: UUID | None,
    requested: frozenset[str],
) -> frozenset[str]:
    return requested


def _client(**resolver_kwargs: Any) -> TestClient:
    ctx = Deps.plain({AuthnDepKey: _AuthnFactory()})

    resolver_kwargs.setdefault("header_name", "Authorization")
    router = APIRouter()
    attach_realtime_ws_route(
        router,
        ctx_dep=lambda: context_from_deps(ctx),
        resolve=build_ws_connection_resolver(
            ctx_dep=lambda: context_from_deps(ctx),
            authn_spec=_SPEC,
            **resolver_kwargs,
        ),
        mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
        cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
        authorize_topics=_allow_all,
    )

    app = FastAPI()
    app.include_router(router)

    return TestClient(app)


def _is_live(ws: Any) -> bool:
    """A connected socket answers an unknown frame with an error rather than closing."""

    ws.send_text(json.dumps({"type": "nope"}))

    return ws.receive_json()["type"] == "error"


# ----------------------- #


class TestCookieMode:
    def test_a_browser_shaped_upgrade_authenticates(self) -> None:
        # No header, no query — exactly what a browser `new WebSocket(...)` sends.
        client = _client(cookie_name=_COOKIE, origin_allowlist_attested=True)
        client.cookies.set(_COOKIE, _GOOD)

        with client.websocket_connect("/realtime/ws") as ws:
            assert _is_live(ws)

    def test_the_credentials_expiry_closes_the_socket(self) -> None:
        client = _client(cookie_name=_COOKIE, origin_allowlist_attested=True)
        client.cookies.set(_COOKIE, _SHORT_LIVED)

        with client.websocket_connect("/realtime/ws") as ws:
            assert _is_live(ws)

            with pytest.raises(WebSocketDisconnect) as caught:
                ws.receive_json()  # nothing to deliver — the expiry guard closes it

        assert caught.value.code == 1008
        assert "expired" in str(caught.value.reason).lower()

    def test_cookie_mode_without_the_origin_attestation_is_refused_at_construction(self) -> None:
        # The factory cannot see the route's allowed_origins — they meet at the attach
        # call — so the attestation is the only thing standing between a shipped
        # resolver and a framework-packaged CSRF hole.
        with pytest.raises(CoreException) as caught:
            build_ws_connection_resolver(
                ctx_dep=lambda: None,  # pyright: ignore[reportArgumentType]
                authn_spec=_SPEC,
                cookie_name=_COOKIE,
            )

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "realtime_cookie_origin_unattested"
        assert "allowed_origins" in str(caught.value)

    def test_the_header_only_default_needs_no_attestation(self) -> None:
        # Header and query carry no ambient credential, so they carry no CSRF exposure.
        assert build_ws_connection_resolver(
            ctx_dep=lambda: None,  # pyright: ignore[reportArgumentType]
            authn_spec=_SPEC,
        )


class TestLadderOnALiveUpgrade:
    def test_the_cookie_outranks_the_header(self) -> None:
        client = _client(cookie_name=_COOKIE, origin_allowlist_attested=True)
        client.cookies.set(_COOKIE, _REVOKED)

        # The header carries a perfectly good token; the presented cookie still decides.
        with (
            client.websocket_connect(
                "/realtime/ws", headers={"Authorization": f"Bearer {_GOOD}"}
            ) as ws,
            pytest.raises(WebSocketDisconnect) as caught,
        ):
            ws.receive_json()

        assert caught.value.code == 1008
        assert "Invalid access token" in str(caught.value.reason)

    def test_the_header_authenticates_a_non_browser_client(self) -> None:
        client = _client()

        with client.websocket_connect(
            "/realtime/ws", headers={"Authorization": f"Bearer {_GOOD}"}
        ) as ws:
            assert _is_live(ws)

    def test_a_query_token_authenticates_nothing_by_default(self) -> None:
        client = _client()

        with (
            client.websocket_connect(f"/realtime/ws?token={_GOOD}") as ws,
            pytest.raises(WebSocketDisconnect) as caught,
        ):
            ws.receive_json()

        assert caught.value.code == 1008
        assert "authenticated principal" in str(caught.value.reason)

    def test_the_query_source_works_once_it_is_opted_into(self) -> None:
        client = _client(query_param="token")

        with client.websocket_connect(f"/realtime/ws?token={_GOOD}") as ws:
            assert _is_live(ws)


class TestReauth:
    def test_a_fresh_payload_token_extends_the_connection_in_place(self) -> None:
        client = _client()

        with client.websocket_connect(
            "/realtime/ws", headers={"Authorization": f"Bearer {_SHORT_LIVED}"}
        ) as ws:
            ws.send_text(json.dumps({"type": "realtime.reauth", "auth": {"token": _GOOD}}))

            assert ws.receive_json() == {"type": "ack", "cid": None, "data": {"ok": True}}

            # Past the short-lived credential's own expiry the socket is still live —
            # the whole point of a reauth: no reconnect, no replay.
            time.sleep(0.6)

            assert _is_live(ws)

    def test_a_reauth_for_another_principal_is_refused(self) -> None:
        # The swap is an identity change on a live socket, so the route refuses it —
        # pinned through the shipped resolver, which is what makes the two tokens
        # resolve to two principals in the first place.
        client = _client()

        with client.websocket_connect(
            "/realtime/ws", headers={"Authorization": f"Bearer {_GOOD}"}
        ) as ws:
            ws.send_text(
                json.dumps({"type": "realtime.reauth", "auth": {"token": _OTHER_PRINCIPAL_TOKEN}})
            )
            frame = ws.receive_json()

            assert frame["type"] == "ack"
            assert "same principal" in frame["error"]["detail"]
            assert _is_live(ws)  # refused, not closed

    def test_a_revoked_reauth_token_is_refused(self) -> None:
        client = _client()

        with client.websocket_connect(
            "/realtime/ws", headers={"Authorization": f"Bearer {_GOOD}"}
        ) as ws:
            ws.send_text(json.dumps({"type": "realtime.reauth", "auth": {"token": _REVOKED}}))
            frame = ws.receive_json()

            assert frame["type"] == "ack"
            assert frame["error"]["code"] == "invalid_access_token"


class TestTheTokenStaysOutOfWhatIsWrittenDown:
    def test_neither_the_close_reason_nor_the_logs_carry_the_credential(self) -> None:
        client = _client(query_param="token")
        secret = "s3cret-but-unknown"

        with (
            structlog.testing.capture_logs() as logs,
            client.websocket_connect(f"/realtime/ws?token={secret}") as ws,
            pytest.raises(WebSocketDisconnect) as caught,
        ):
            ws.receive_json()

        assert caught.value.code == 1008
        assert secret not in str(caught.value.reason)
        assert secret not in json.dumps(logs, default=str)


class TestClientIdentityOnTheUpgrade:
    def test_the_device_id_query_parameter_reaches_the_connection(self) -> None:
        # The per-device cursor key is what makes offline replay resume correctly; a
        # resolver that drops it silently degrades every reconnect to a fresh cursor.
        seen: list[Any] = []

        ctx = Deps.plain({AuthnDepKey: _AuthnFactory()})

        resolver = build_ws_connection_resolver(
            ctx_dep=lambda: context_from_deps(ctx),
            authn_spec=_SPEC,
        )
        router = APIRouter()

        async def _record(connect: Any) -> Any:
            # `WsConnectionResolver` allows a sync resolver, so the route's own await
            # dance is what a caller has to do — the shipped one is always async.
            outcome = resolver(connect)
            resolved = await outcome if isawaitable(outcome) else outcome
            seen.append(resolved)

            return resolved

        attach_realtime_ws_route(
            router,
            ctx_dep=lambda: context_from_deps(ctx),
            resolve=_record,
            mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
            cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
            authorize_topics=_allow_all,
        )
        app = FastAPI()
        app.include_router(router)

        with TestClient(app).websocket_connect(
            "/realtime/ws?device_id=phone-7",
            headers={"Authorization": f"Bearer {_GOOD}"},
        ) as ws:
            assert _is_live(ws)

        assert seen and seen[0] is not None
        assert seen[0].client is not None
        assert seen[0].client.device_id == "phone-7"
        assert seen[0].expires_at == datetime(2027, 1, 1, tzinfo=UTC)


class TestTheTenantIsBound:
    def test_the_routes_tenancy_resolver_decides_the_connections_tenant(self) -> None:
        # A tenant-aware adapter fails closed without this, so a connection that
        # resolves no tenant is a correctly authenticated socket that can read nothing.
        tenant = UUID("33333333-3333-3333-3333-333333333333")
        seen: list[Any] = []

        class _Resolver:
            async def resolve_from_principal(
                self,
                principal_id: UUID,
                *,
                requested_tenant_id: UUID | None = None,
            ) -> TenantIdentity:
                _ = principal_id, requested_tenant_id

                return TenantIdentity(tenant_id=tenant)

        # Registered **routed**, the shape `TenancyDepsModule` actually emits: a
        # route-less lookup finds nothing there, and the socket would bind no tenant
        # on a credential that authenticated perfectly.
        deps = (
            Deps.plain({AuthnDepKey: _AuthnFactory()}),
            Deps.routed({TenantResolverDepKey: {str(_SPEC.name): lambda _ctx: _Resolver()}}),
        )
        resolver = build_ws_connection_resolver(
            ctx_dep=lambda: context_from_deps(*deps),
            authn_spec=_SPEC,
        )
        router = APIRouter()

        async def _record(connect: Any) -> Any:
            outcome = resolver(connect)
            resolved = await outcome if isawaitable(outcome) else outcome
            seen.append(resolved)

            return resolved

        attach_realtime_ws_route(
            router,
            ctx_dep=lambda: context_from_deps(*deps),
            resolve=_record,
            mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
            cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
            authorize_topics=_allow_all,
        )
        app = FastAPI()
        app.include_router(router)

        with TestClient(app).websocket_connect(
            "/realtime/ws", headers={"Authorization": f"Bearer {_GOOD}"}
        ) as ws:
            assert _is_live(ws)

        assert seen and seen[0] is not None
        assert seen[0].tenant == tenant


    def test_a_plainly_registered_resolver_is_still_found(self) -> None:
        # Defaulting the route must not cost the other wiring: a routed lookup falls
        # back to a plain registration, and this is the test that keeps it true.
        tenant = UUID("55555555-5555-5555-5555-555555555555")
        seen: list[Any] = []

        class _Resolver:
            async def resolve_from_principal(
                self,
                principal_id: UUID,
                *,
                requested_tenant_id: UUID | None = None,
            ) -> TenantIdentity:
                _ = principal_id, requested_tenant_id

                return TenantIdentity(tenant_id=tenant)

        deps = Deps.plain(
            {AuthnDepKey: _AuthnFactory(), TenantResolverDepKey: lambda _ctx: _Resolver()}
        )
        resolver = build_ws_connection_resolver(
            ctx_dep=lambda: context_from_deps(deps),
            authn_spec=_SPEC,
        )
        router = APIRouter()

        async def _record(connect: Any) -> Any:
            outcome = resolver(connect)
            resolved = await outcome if isawaitable(outcome) else outcome
            seen.append(resolved)

            return resolved

        attach_realtime_ws_route(
            router,
            ctx_dep=lambda: context_from_deps(deps),
            resolve=_record,
            mailbox_factory=lambda _ctx: InMemoryRealtimeMailbox(),
            cursors_factory=lambda _ctx: InMemoryMailboxCursors(),
            authorize_topics=_allow_all,
        )
        app = FastAPI()
        app.include_router(router)

        with TestClient(app).websocket_connect(
            "/realtime/ws", headers={"Authorization": f"Bearer {_GOOD}"}
        ) as ws:
            assert _is_live(ws)

        assert seen and seen[0] is not None
        assert seen[0].tenant == tenant


class TestAnonymous:
    def test_an_upgrade_with_no_credential_at_all_is_refused(self) -> None:
        client = _client()

        with (
            client.websocket_connect("/realtime/ws") as ws,
            pytest.raises(WebSocketDisconnect) as caught,
        ):
            ws.receive_json()

        assert caught.value.code == 1008
        assert "authenticated principal" in str(caught.value.reason)
