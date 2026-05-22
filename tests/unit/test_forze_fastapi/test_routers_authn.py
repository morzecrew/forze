"""Unit tests for ``forze_fastapi.endpoints.authn``."""

from __future__ import annotations

from typing import Any

import attrs
import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.composition.authn import (
    AuthnKernelOp,
)
from forze.application.contracts.authn import AuthnSpec
from forze.application.handlers.authn import (
    AuthnChangePasswordRequestDTO,
    AuthnLoginRequestDTO,
    AuthnRefreshRequestDTO,
    AuthnTokenResponseDTO,
)
from forze.application.execution import ExecutionContext
from forze.application.execution.registry import OperationRegistry
from registry_helpers import freeze_registry

from forze_fastapi.endpoints.authn import (
    CookieTokenTransportSpec,
    HeaderTokenTransportSpec,
    attach_authn_endpoints,
)
from forze_mock import MockDepsModule, MockState

# ----------------------- #


pytestmark = pytest.mark.unit


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _StubLogin:
    response: AuthnTokenResponseDTO

    async def __call__(self, args: AuthnLoginRequestDTO) -> AuthnTokenResponseDTO:
        _ = args
        return self.response


@attrs.define(slots=True, kw_only=True, frozen=True)
class _StubRefresh:
    response: AuthnTokenResponseDTO
    captured: list[str]

    async def __call__(self, args: AuthnRefreshRequestDTO) -> AuthnTokenResponseDTO:
        self.captured.append(args.refresh_token)
        return self.response


@attrs.define(slots=True, kw_only=True, frozen=True)
class _StubLogout:
    called: list[bool]

    async def __call__(self, args: None) -> None:
        _ = args
        self.called.append(True)


@attrs.define(slots=True, kw_only=True, frozen=True)
class _StubChangePassword:
    captured: list[str]

    async def __call__(self, args: AuthnChangePasswordRequestDTO) -> None:
        self.captured.append(args.new_password)


# ....................... #


def _login_response() -> AuthnTokenResponseDTO:
    return AuthnTokenResponseDTO(
        access_token="ACCESS-1",
        refresh_token="REFRESH-1",
        access_token_type="Bearer",
        access_expires_in=900,
        refresh_expires_in=86400,
    )


def _spec() -> AuthnSpec:
    return AuthnSpec(name="main", enabled_methods=("password",))


def _make_registry(
    *,
    login_response: AuthnTokenResponseDTO | None = None,
    refresh_response: AuthnTokenResponseDTO | None = None,
    refresh_capture: list[str] | None = None,
    logout_calls: list[bool] | None = None,
    change_password_capture: list[str] | None = None,
):
    spec = _spec()
    ns = spec.default_namespace
    factories: dict[str, Any] = {}
    bound_ops: list[str] = []

    if login_response is not None:
        op = ns.key(AuthnKernelOp.PASSWORD_LOGIN)
        factories[op] = lambda _ctx, r=login_response: _StubLogin(response=r)
        bound_ops.append(op)

    if refresh_response is not None:
        rc = refresh_capture if refresh_capture is not None else []
        op = ns.key(AuthnKernelOp.REFRESH_TOKENS)
        factories[op] = lambda _ctx, r=refresh_response, c=rc: _StubRefresh(
            response=r,
            captured=c,
        )
        bound_ops.append(op)

    if logout_calls is not None:
        op = ns.key(AuthnKernelOp.LOGOUT)
        factories[op] = lambda _ctx, calls=logout_calls: _StubLogout(called=calls)
        bound_ops.append(op)

    if change_password_capture is not None:
        op = ns.key(AuthnKernelOp.CHANGE_PASSWORD)
        factories[op] = lambda _ctx, cap=change_password_capture: _StubChangePassword(
            captured=cap,
        )
        bound_ops.append(op)

    reg = OperationRegistry(handlers=factories)
    return freeze_registry(reg, ops=bound_ops)


def _ctx_dep(ctx: ExecutionContext):
    def _get() -> ExecutionContext:
        return ctx

    return _get


@pytest.fixture
def authn_ctx(composition_mock_state: MockState) -> ExecutionContext:
    deps = MockDepsModule(state=composition_mock_state)()
    return ExecutionContext(deps=deps)


# ....................... #


class TestPasswordLoginHeaderTransport:
    def test_header_transport_returns_tokens_in_body(
        self,
        authn_ctx: ExecutionContext,
    ) -> None:
        reg = _make_registry(login_response=_login_response())

        router = APIRouter(prefix="/auth")
        attach_authn_endpoints(
            router,
            spec=_spec(),
            registry=reg,
            ctx_dep=_ctx_dep(authn_ctx),
            endpoints={"password_login": True},
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        res = client.post(
            "/auth/login",
            data={"login": "alice", "password": "pw"},
        )

        assert res.status_code == 200
        body = res.json()
        assert body["access_token"] == "ACCESS-1"
        assert body["refresh_token"] == "REFRESH-1"
        assert body["access_token_type"] == "Bearer"
        assert body["access_expires_in"] == 900


class TestPasswordLoginCookieRefreshTransport:
    def test_cookie_refresh_strips_token_from_body_and_sets_cookie(
        self,
        authn_ctx: ExecutionContext,
    ) -> None:
        reg = _make_registry(login_response=_login_response())

        router = APIRouter(prefix="/auth")
        attach_authn_endpoints(
            router,
            spec=_spec(),
            registry=reg,
            ctx_dep=_ctx_dep(authn_ctx),
            endpoints={
                "password_login": True,
                "config": {
                    "access_token_transport": HeaderTokenTransportSpec(
                        kind="header",
                        header_name="Authorization",
                        scheme="Bearer",
                    ),
                    "refresh_token_transport": CookieTokenTransportSpec(
                        kind="cookie",
                        cookie_name="rt",
                        cookie_secure=False,
                    ),
                },
            },
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        res = client.post(
            "/auth/login",
            data={"login": "alice", "password": "pw"},
        )

        assert res.status_code == 200
        body = res.json()

        assert body["access_token"] == "ACCESS-1"
        assert body.get("refresh_token") is None  # stripped, lives in cookie
        assert "rt" in res.cookies
        assert res.cookies["rt"] == "REFRESH-1"


class TestRefreshEndpoint:
    def test_reads_refresh_token_from_cookie(
        self,
        authn_ctx: ExecutionContext,
    ) -> None:
        captured: list[str] = []
        reg = _make_registry(
            refresh_response=_login_response(),
            refresh_capture=captured,
        )

        router = APIRouter(prefix="/auth")
        attach_authn_endpoints(
            router,
            spec=_spec(),
            registry=reg,
            ctx_dep=_ctx_dep(authn_ctx),
            endpoints={
                "refresh": True,
                "config": {
                    "refresh_token_transport": CookieTokenTransportSpec(
                        kind="cookie",
                        cookie_name="rt",
                        cookie_secure=False,
                    ),
                },
            },
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        res = client.post("/auth/refresh", cookies={"rt": "OLD-REFRESH"})

        assert res.status_code == 200
        assert captured == ["OLD-REFRESH"]

    def test_refresh_missing_token_returns_401(
        self,
        authn_ctx: ExecutionContext,
    ) -> None:
        reg = _make_registry(
            refresh_response=_login_response(),
            refresh_capture=[],
        )

        router = APIRouter(prefix="/auth")
        attach_authn_endpoints(
            router,
            spec=_spec(),
            registry=reg,
            ctx_dep=_ctx_dep(authn_ctx),
            endpoints={
                "refresh": True,
                "config": {
                    "refresh_token_transport": CookieTokenTransportSpec(
                        kind="cookie",
                        cookie_name="rt",
                        cookie_secure=False,
                    ),
                },
            },
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        res = client.post("/auth/refresh")
        assert res.status_code == 401

    def test_reads_refresh_token_from_header(
        self,
        authn_ctx: ExecutionContext,
    ) -> None:
        captured: list[str] = []
        reg = _make_registry(
            refresh_response=_login_response(),
            refresh_capture=captured,
        )

        router = APIRouter(prefix="/auth")
        attach_authn_endpoints(
            router,
            spec=_spec(),
            registry=reg,
            ctx_dep=_ctx_dep(authn_ctx),
            endpoints={
                "refresh": True,
                "config": {
                    "refresh_token_transport": HeaderTokenTransportSpec(
                        kind="header",
                        header_name="X-Refresh-Token",
                        scheme="",
                    ),
                },
            },
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        res = client.post(
            "/auth/refresh",
            headers={"X-Refresh-Token": "OLD-REFRESH"},
        )

        assert res.status_code == 200
        assert captured == ["OLD-REFRESH"]


class TestChangePassword:
    def test_change_password_form_body(
        self,
        authn_ctx: ExecutionContext,
    ) -> None:
        captured: list[str] = []
        reg = _make_registry(change_password_capture=captured)

        router = APIRouter(prefix="/auth")
        attach_authn_endpoints(
            router,
            spec=_spec(),
            registry=reg,
            ctx_dep=_ctx_dep(authn_ctx),
            endpoints={"change_password": True},
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        res = client.post(
            "/auth/change-password",
            data={"new_password": "new-pw"},
        )

        # Authn requirement is auto-applied (RequireAuthnFeature) and there
        # is no identity bound on the test ctx, so this returns 401.
        assert res.status_code == 401


class TestLogout:
    def test_logout_returns_204_when_authenticated(
        self,
        authn_ctx: ExecutionContext,
    ) -> None:
        # Bind a fake identity on the context so RequireAuthnFeature passes.
        from uuid import uuid4

        from forze.application.contracts.authn import AuthnIdentity
        from forze.application.execution import InvocationMetadata

        called: list[bool] = []
        reg = _make_registry(logout_calls=called)

        def _invocation_metadata() -> InvocationMetadata:
            return InvocationMetadata(
                execution_id=uuid4(),
                correlation_id=uuid4(),
            )

        router = APIRouter(prefix="/auth")
        attach_authn_endpoints(
            router,
            spec=_spec(),
            registry=reg,
            ctx_dep=_ctx_dep(authn_ctx),
            endpoints={
                "logout": True,
                "config": {
                    "access_token_transport": CookieTokenTransportSpec(
                        kind="cookie",
                        cookie_name="at",
                        cookie_secure=False,
                    ),
                },
            },
        )

        app = FastAPI()

        # Bind authn identity for the duration of the request via dependency.
        @app.middleware("http")
        async def _bind_identity(request, call_next):  # type: ignore[no-untyped-def]
            identity = AuthnIdentity(principal_id=uuid4())
            with authn_ctx.inv.bind(
                metadata=_invocation_metadata(),
                authn=identity,
            ):
                return await call_next(request)

        app.include_router(router)
        client = TestClient(app)

        res = client.post("/auth/logout", cookies={"at": "STALE"})
        assert res.status_code == 204
        assert called == [True]
        # Cookie deletion sets it to empty value with Max-Age=0 in Set-Cookie
        # header; TestClient applies it so the cookie jar is now empty.
        assert "at" not in client.cookies or client.cookies.get("at") in ("", None)
