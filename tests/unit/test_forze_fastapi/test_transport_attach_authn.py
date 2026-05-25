"""Unit tests for ``attach_authn_routes``."""

from __future__ import annotations


import attrs
import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.composition.authn import AuthnFacade, AuthnKernelOp
from forze.application.contracts.authn import AuthnSpec
from forze.application.execution.registry import OperationRegistry
from forze.application.handlers.authn import (
    AuthnLoginRequestDTO,
    AuthnTokenResponseDTO,
)
from forze_fastapi.transport.http import attach_authn_routes, make_facade_dep
from forze_fastapi.transport.http.wire.authn import (
    CookieTokenTransportSpec,
    HeaderTokenTransportSpec,
)
from registry_helpers import freeze_registry

pytestmark = pytest.mark.unit


@attrs.define(slots=True, kw_only=True, frozen=True)
class _StubLogin:
    response: AuthnTokenResponseDTO

    async def __call__(self, args: AuthnLoginRequestDTO) -> AuthnTokenResponseDTO:
        _ = args
        return self.response


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


def _make_registry(login_response: AuthnTokenResponseDTO):
    spec = _spec()
    op = spec.default_namespace.key(AuthnKernelOp.PASSWORD_LOGIN)
    reg = OperationRegistry(
        handlers={op: lambda _ctx, r=login_response: _StubLogin(response=r)},
    )
    return freeze_registry(reg), spec


class TestAttachAuthnRoutes:
    def test_password_login_header_transport(self, composition_ctx) -> None:
        reg, spec = _make_registry(_login_response())

        def ctx_dep():
            return composition_ctx

        facade_dep = make_facade_dep(
            AuthnFacade,
            registry=reg,
            namespace=spec.default_namespace,
            ctx_dep=ctx_dep,
        )
        router = APIRouter(prefix="/auth")
        attach_authn_routes(
            router,
            spec=spec,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
            enable=("password_login",),
        )
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        res = client.post("/auth/login", data={"login": "alice", "password": "pw"})
        assert res.status_code == 200
        body = res.json()
        assert body["access_token"] == "ACCESS-1"
        assert body["refresh_token"] == "REFRESH-1"

    def test_cookie_refresh_transport(self, composition_ctx) -> None:
        reg, spec = _make_registry(_login_response())

        def ctx_dep():
            return composition_ctx

        facade_dep = make_facade_dep(
            AuthnFacade,
            registry=reg,
            namespace=spec.default_namespace,
            ctx_dep=ctx_dep,
        )
        router = APIRouter(prefix="/auth")
        attach_authn_routes(
            router,
            spec=spec,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
            enable=("password_login",),
            config={
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
        )
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        res = client.post("/auth/login", data={"login": "alice", "password": "pw"})
        assert res.status_code == 200
        body = res.json()
        assert body["access_token"] == "ACCESS-1"
        assert body.get("refresh_token") is None
        assert res.cookies["rt"] == "REFRESH-1"
