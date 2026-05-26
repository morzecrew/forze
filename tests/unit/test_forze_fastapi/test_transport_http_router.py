"""Unit tests for ``forze_fastapi.transport.http`` router and policies (PR2)."""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

import attrs
import pytest
from fastapi import Depends, FastAPI
from pydantic import BaseModel
from registry_helpers import freeze_registry
from starlette.testclient import TestClient

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnDepKey,
    AuthnIdentity,
    AuthnResult,
    AuthnSpec,
)
from forze.application.contracts.execution import Handler
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.registry import OperationRegistry
from forze.application.execution.running import run_operation
from forze_fastapi.middlewares.context import HeaderTokenAuthnIdentityResolver
from forze_fastapi.middlewares.context.middleware import ContextBindingMiddleware
from forze_fastapi.transport.http import (
    AuthnRequirement,
    ForzeRouter,
    RequirePrincipal,
)

# ----------------------- #

pytestmark = pytest.mark.unit

_AUTHN_SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"token"}))


@pytest.fixture
def authn_ctx() -> ExecutionContext:
    return ExecutionContext(deps=Deps.plain({AuthnDepKey: _TokenAuthFactory()}))


class _EchoBody(BaseModel):
    message: str


@attrs.define(slots=True, kw_only=True, frozen=True)
class _EchoHandler(Handler[_EchoBody, _EchoBody]):
    async def __call__(self, args: _EchoBody) -> _EchoBody:
        return args


class _TokenAuthPort:
    async def authenticate_with_password(
        self, credentials: object
    ) -> AuthnResult | None:
        return None

    async def authenticate_with_token(
        self,
        credentials: AccessTokenCredentials,
    ) -> AuthnResult | None:
        return AuthnResult(
            identity=AuthnIdentity(principal_id=uuid5(NAMESPACE_URL, credentials.token))
        )

    async def authenticate_with_api_key(
        self, credentials: object
    ) -> AuthnResult | None:
        return None


class _TokenAuthFactory:
    def __call__(self, _ctx: ExecutionContext, _spec: AuthnSpec) -> _TokenAuthPort:
        return _TokenAuthPort()


# ....................... #


class TestAuthnRequirementTransport:
    def test_token_header_branch(self) -> None:
        req = AuthnRequirement(authn_route="main", token_header="Authorization")
        assert req.scheme_kind == "bearer"
        assert req.scheme_name == "forze_authn__main__bearer"

    def test_mutually_exclusive_transports_raises(self) -> None:
        with pytest.raises(exc.internal, match="exactly one of"):
            AuthnRequirement(
                authn_route="main",
                token_header="Authorization",
                token_cookie="access_token",
            )


# ....................... #


class TestRequirePrincipalRoute:
    def test_returns_401_when_principal_not_bound(
        self,
        composition_ctx: ExecutionContext,
    ) -> None:
        def ctx_dep() -> ExecutionContext:
            return composition_ctx

        req = AuthnRequirement(authn_route="main", token_header="Authorization")
        router = ForzeRouter(
            policies=[RequirePrincipal(requirement=req, ctx_dep=ctx_dep)],
        )

        @router.forze_route(method="GET", path="/protected")
        async def protected() -> dict[str, str]:
            return {"ok": "true"}

        app = FastAPI()
        app.include_router(router)

        with TestClient(app) as client:
            response = client.get("/protected")

        assert response.status_code == 401

    def test_returns_200_when_middleware_bound_identity(
        self,
        authn_ctx: ExecutionContext,
    ) -> None:
        def ctx_dep() -> ExecutionContext:
            return authn_ctx

        req = AuthnRequirement(authn_route="main", token_header="Authorization")
        router = ForzeRouter(
            policies=[RequirePrincipal(requirement=req, ctx_dep=ctx_dep)],
        )

        @router.forze_route(method="GET", path="/protected")
        async def protected() -> dict[str, str]:
            ident = authn_ctx.inv.get_authn()
            assert ident is not None
            return {"principal_id": str(ident.principal_id)}

        app = FastAPI()
        app.add_middleware(
            ContextBindingMiddleware,
            ctx_dep=ctx_dep,
            authn_identity_resolvers=[
                HeaderTokenAuthnIdentityResolver(spec=_AUTHN_SPEC),
            ],
        )
        app.include_router(router)

        with TestClient(app) as client:
            response = client.get(
                "/protected",
                headers={"Authorization": "Bearer secret-token"},
            )

        assert response.status_code == 200
        assert "principal_id" in response.json()

    def test_openapi_lists_security_scheme(
        self,
        composition_ctx: ExecutionContext,
    ) -> None:
        def ctx_dep() -> ExecutionContext:
            return composition_ctx

        req = AuthnRequirement(authn_route="main", token_header="Authorization")
        router = ForzeRouter(
            policies=[RequirePrincipal(requirement=req, ctx_dep=ctx_dep)],
        )

        @router.forze_route(method="GET", path="/protected")
        async def protected() -> None:
            return None

        app = FastAPI()
        app.include_router(router)
        schema = app.openapi()
        schemes = schema["components"]["securitySchemes"]
        assert "forze_authn__main__bearer" in schemes


# ....................... #


class TestCustomRunOperationRoute:
    async def test_forze_route_run_operation(
        self,
        composition_ctx: ExecutionContext,
    ) -> None:
        reg = OperationRegistry(
            handlers={"custom.echo": lambda _ctx: _EchoHandler()},
        )
        frozen = freeze_registry(reg)

        def ctx_dep() -> ExecutionContext:
            return composition_ctx

        router = ForzeRouter()

        @router.forze_route(method="POST", path="/echo", operation="custom.echo")
        async def echo(
            body: _EchoBody,
            ctx: ExecutionContext = Depends(ctx_dep),
        ) -> _EchoBody:
            return await run_operation(frozen, "custom.echo", body, ctx=ctx)

        app = FastAPI()
        app.include_router(router)

        with TestClient(app) as client:
            response = client.post("/echo", json={"message": "hi"})

        assert response.status_code == 200
        assert response.json() == {"message": "hi"}
