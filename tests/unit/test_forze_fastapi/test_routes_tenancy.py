"""Route-surface tests for ``attach_tenancy_routes`` (the tenant selector)."""

from __future__ import annotations

import asyncio
import secrets
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.authn import AuthnSpec, PasswordCredentials
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.application.hooks.tenancy import TenantRequired
from forze.base.exceptions import CoreException
from forze.domain.models import ReadDocument
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.middlewares import SecurityContextMiddleware, check_authn_cookie_paths
from forze_fastapi.routes import (
    RouteBinding,
    attach_authn_routes,
    attach_operation_routes,
    attach_tenancy_routes,
    id_endpoint,
)
from forze_fastapi.routes._attach import PATH_PARAMS_ATTR
from forze_fastapi.routes.authn import _cookie_token_endpoint
from forze_fastapi.security import (
    AuthnCookieCarrier,
    AuthnRequirement,
    CookieTokenAuthn,
    HeaderTokenAuthn,
)
from forze_identity.tenancy import TenancyDepsModule, principal_tenant_binding_spec, tenant_spec
from forze_kits.aggregates.authn import build_authn_registry
from forze_kits.aggregates.tenancy import TenancyKernelOp, build_tenancy_registry
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters.identity import seed_password_account
from forze_mock.execution.configs import MockRouteConfig
from forze_mock.execution.factories import ConfigurableMockDocument
from tests.support.authn_stacks import AUTHN_ROUTE, identity_authn_context, stack_principal
from tests.support.execution_context import context_from_modules

# ----------------------- #

_SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"password", "token"}))
_NS = _SPEC.default_namespace


def _build_app(*, include: Any = None) -> FastAPI:
    reg = build_tenancy_registry(_SPEC).freeze()
    router = APIRouter(prefix="/auth")
    attach_tenancy_routes(
        router, registry=reg, ns=_NS, ctx_dep=lambda: None, include=include
    )
    app = FastAPI()
    app.include_router(router)
    return app


def _op_ids(app: FastAPI) -> set[str]:
    return {
        meta["operationId"]
        for methods in app.openapi()["paths"].values()
        for meta in methods.values()
    }


# ....................... #


class TestTenancyRouteSurface:
    def test_routes_are_list_activate_and_leave(self) -> None:
        paths = _build_app().openapi()["paths"]

        assert set(paths) == {
            "/auth/tenants",
            "/auth/tenants/{id}/activate",
            "/auth/tenants/{id}",
        }
        assert set(paths["/auth/tenants"]) == {"get"}
        assert set(paths["/auth/tenants/{id}/activate"]) == {"post"}
        assert set(paths["/auth/tenants/{id}"]) == {"delete"}

    def test_operation_ids_are_registry_keys_verbatim(self) -> None:
        assert _op_ids(_build_app()) == {f"main.{op.value}" for op in TenancyKernelOp}

    def test_the_tenant_id_is_described(self) -> None:
        paths = _build_app().openapi()["paths"]

        for path, method in (
            ("/auth/tenants/{id}/activate", "post"),
            ("/auth/tenants/{id}", "delete"),
        ):
            (param,) = paths[path][method]["parameters"]

            assert (param["name"], param["in"]) == ("id", "path")
            assert param["description"].startswith("The tenant ")

    def test_both_routes_flagged_requires_authn(self) -> None:
        paths = _build_app().openapi()["paths"]

        for methods in paths.values():
            for meta in methods.values():
                assert meta.get("x-requires-authn") is True

    def test_include_narrows_to_subset(self) -> None:
        app = _build_app(include={TenancyKernelOp.LIST_TENANTS})

        assert set(app.openapi()["paths"]) == {"/auth/tenants"}


# ....................... #


class _NoteRead(ReadDocument):
    title: str = ""


# A tenant-aware store: what a tenant-scoped operation reads.
_NOTES = DocumentSpec(name="notes", read=_NoteRead)
_LOGIN_SPEC = AuthnSpec(name=AUTHN_ROUTE, enabled_methods=frozenset({"password", "token"}))


def _selector_context() -> ExecutionContext:
    """The identity plane's authn and tenancy adapters over mock documents."""

    tenancy_docs = ConfigurableMockDocument(module=MockDepsModule())
    notes = ConfigurableMockDocument(
        module=MockDepsModule(routes={"notes": MockRouteConfig(tenant_aware=True)})
    )
    names = {tenant_spec.name, principal_tenant_binding_spec.name}
    tenancy = TenancyDepsModule(
        tenant_resolver={AUTHN_ROUTE},
        tenant_management={AUTHN_ROUTE},
    )()

    return identity_authn_context(
        tenancy.merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: {
                        **dict.fromkeys(names, tenancy_docs),
                        _NOTES.name: notes,
                    },
                    DocumentCommandDepKey: dict.fromkeys(names, tenancy_docs),
                }
            )
        )
    )


def _selector_app(ctx: ExecutionContext) -> FastAPI:
    router = APIRouter(prefix="/auth")
    ns = _LOGIN_SPEC.default_namespace
    attach_authn_routes(
        router,
        registry=build_authn_registry(_LOGIN_SPEC).freeze(),
        ns=ns,
        ctx_dep=lambda: ctx,
    )
    attach_tenancy_routes(
        router,
        registry=build_tenancy_registry(_LOGIN_SPEC).freeze(),
        ns=ns,
        ctx_dep=lambda: ctx,
    )

    app = FastAPI()
    app.include_router(router)

    @app.get("/scoped/guard")
    async def guard() -> None:
        await TenantRequired()(ctx)(None)

    @app.get("/scoped/notes")
    async def read_notes() -> None:
        await ctx.doc.query(_NOTES).find_many()

    register_exception_handlers(app)
    app.add_middleware(
        SecurityContextMiddleware,  # type: ignore[arg-type]
        ctx_dep=lambda: ctx,
        authn=AuthnRequirement(
            ingress=(HeaderTokenAuthn(authn_spec=_LOGIN_SPEC, header_name="Authorization"),),
        ),
        when_multiple_credentials="first_in_order",
        anonymous_paths={"/auth/login"},
    )

    return app


def _member_of_two(ctx: ExecutionContext, password: str) -> tuple[UUID, UUID]:
    """A password principal attached to two fresh tenants."""

    async def seed() -> tuple[UUID, UUID]:
        principal = await stack_principal(ctx)
        await ctx.authn.password_account_provisioning(_LOGIN_SPEC).register_with_password(
            principal, PasswordCredentials(login="alice", password=password)
        )
        mgmt = ctx.tenancy.manager(AUTHN_ROUTE)
        assert mgmt is not None
        first = await mgmt.provision_tenant(tenant_key="acme")
        second = await mgmt.provision_tenant(tenant_key="globex")
        await mgmt.attach_principal(principal, first.tenant_id)
        await mgmt.attach_principal(principal, second.tenant_id)

        return first.tenant_id, second.tenant_id

    return asyncio.run(seed())


class TestTenantSelectorForSeveralMemberships:
    """A password login carries no tenant, so a principal in two tenants reaches the
    selector with none bound, and everything tenant-scoped refuses it until it picks one."""

    def _signed_in(self) -> tuple[TestClient, dict[str, str], UUID, UUID]:
        ctx = _selector_context()
        password = secrets.token_hex(16)
        first, second = _member_of_two(ctx, password)
        client = TestClient(_selector_app(ctx))
        login = client.post("/auth/login", json={"login": "alice", "password": password})
        assert login.status_code == 200

        return client, {"Authorization": f"Bearer {login.json()['access_token']}"}, first, second

    def test_the_selector_lists_both_tenants_with_none_current(self) -> None:
        client, auth, first, second = self._signed_in()

        listed = client.get("/auth/tenants", headers=auth)

        assert listed.status_code == 200
        tenants = listed.json()["tenants"]
        assert {UUID(t["tenant_id"]) for t in tenants} == {first, second}
        assert not any(t["is_current"] for t in tenants)

    def test_tenant_scoped_work_is_refused_until_a_tenant_is_picked(self) -> None:
        client, auth, first, _ = self._signed_in()

        for path in ("/scoped/guard", "/scoped/notes"):
            refused = client.get(path, headers=auth)

            assert refused.status_code == 401
            assert refused.headers.get(ERROR_CODE_HEADER) == "tenant_required"

        activated = client.post(f"/auth/tenants/{first}/activate", headers=auth)
        assert activated.status_code == 200
        scoped = {"Authorization": f"Bearer {activated.json()['access_token']}"}

        for path in ("/scoped/guard", "/scoped/notes"):
            assert client.get(path, headers=scoped).status_code == 200

        listed = client.get("/auth/tenants", headers=scoped).json()["tenants"]
        assert [UUID(t["tenant_id"]) for t in listed if t["is_current"]] == [first]

    def test_a_tenant_outside_the_membership_is_still_refused(self) -> None:
        client, auth, _, _ = self._signed_in()
        stranger = uuid4()

        asked = client.get("/auth/tenants", headers={**auth, "X-Tenant-Id": str(stranger)})
        activated = client.post(f"/auth/tenants/{stranger}/activate", headers=auth)

        for refused in (asked, activated):
            assert refused.status_code == 401
            assert refused.headers.get(ERROR_CODE_HEADER) == "tenant_mismatch"


# ....................... #
# Cookie mode: the switch rotates the browser's cookies instead of returning tokens.

_SAME_ORIGIN = {"origin": "http://testserver"}
_CROSS_SITE = {"origin": "https://evil.example"}
_ACTIVATE = "/auth/tenants/{id}/activate"


def _session_app(
    cookies: AuthnCookieCarrier | None, *, middleware: bool = True
) -> tuple[FastAPI, TestClient, UUID, dict[str, Any]]:
    """Authn + tenancy routes over one mock stack, logged in as a member of one tenant.

    Returns the app, its client, the tenant id and the login response body.
    """

    state = MockState()
    password = secrets.token_hex(16)
    principal = uuid4()
    seed_password_account(state, login="alice", password=password, principal_id=principal)
    ctx = context_from_modules(MockDepsModule(state=state))

    async def _join() -> UUID:
        manager = ctx.tenancy.require_manager(_SPEC.name)
        tenant = await manager.provision_tenant()
        await manager.attach_principal(principal, tenant.tenant_id)
        return tenant.tenant_id

    tenant_id = asyncio.run(_join())

    router = APIRouter(prefix="/auth")
    attach_authn_routes(
        router,
        registry=build_authn_registry(_SPEC).freeze(),
        ns=_NS,
        ctx_dep=lambda: ctx,
        cookies=cookies,
    )
    attach_tenancy_routes(
        router,
        registry=build_tenancy_registry(_SPEC).freeze(),
        ns=_NS,
        ctx_dep=lambda: ctx,
        cookies=cookies,
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    if middleware:
        app.add_middleware(
            SecurityContextMiddleware,  # type: ignore[arg-type]
            ctx_dep=lambda: ctx,
            authn=AuthnRequirement(
                ingress=(
                    CookieTokenAuthn(authn_spec=_SPEC, cookie_name="forze_access"),
                    HeaderTokenAuthn(authn_spec=_SPEC, header_name="Authorization"),
                ),
            ),
            when_multiple_credentials="first_in_order",
            anonymous_paths={"/auth/login", "/auth/refresh", "/auth/logout"},
        )

    client = TestClient(app)
    login = client.post(
        "/auth/login", json={"login": "alice", "password": password}, headers=_SAME_ORIGIN
    )
    assert login.status_code == 200

    return app, client, tenant_id, login.json()


def _carrier() -> AuthnCookieCarrier:
    # secure=False: the TestClient speaks plain http and would drop Secure cookies.
    return AuthnCookieCarrier(secure=False, refresh_path="/auth")


def _session(client: TestClient) -> tuple[str | None, str | None]:
    return client.cookies.get("forze_access"), client.cookies.get("forze_refresh")


class TestTenancyCookieMode:
    def test_switch_rotates_both_cookies_and_strips_the_body(self) -> None:
        _, client, tenant_id, _ = _session_app(_carrier())
        before = _session(client)

        response = client.post(_ACTIVATE.format(id=tenant_id), headers=_SAME_ORIGIN)

        assert response.status_code == 200
        body = response.json()
        assert "access_token" not in body and "refresh_token" not in body
        assert body["access_token_type"] == "Bearer"  # scheme + lifetime stay
        assert body["access_expires_in"] is not None

        set_cookies = response.headers.get_list("set-cookie")
        access = next(c for c in set_cookies if c.startswith("forze_access="))
        refresh = next(c for c in set_cookies if c.startswith("forze_refresh="))
        assert "HttpOnly" in access and "HttpOnly" in refresh
        assert "Path=/auth" in refresh

        after = _session(client)
        assert None not in after
        assert after[0] != before[0] and after[1] != before[1]

        # The browser's new session authenticates the next request.
        listed = client.get("/auth/tenants")
        assert listed.status_code == 200
        assert [t["tenant_id"] for t in listed.json()["tenants"]] == [str(tenant_id)]

    @pytest.mark.parametrize("headers", [_CROSS_SITE, {}], ids=["cross-site", "no-origin"])
    def test_cookie_authenticated_switch_without_origin_proof_is_refused(
        self, headers: dict[str, str]
    ) -> None:
        _, client, tenant_id, _ = _session_app(_carrier())
        before = _session(client)

        refused = client.post(_ACTIVATE.format(id=tenant_id), headers=headers)

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"
        assert not refused.headers.get_list("set-cookie")
        assert _session(client) == before

    def test_a_header_authenticated_switch_is_gated_too(self) -> None:
        # The response sets the session cookies whoever sent it, so the route applies the
        # access cookie's gate itself, as login does: here no cookie triggers the ingress's.
        _, client, tenant_id, _ = _session_app(_carrier())
        bearer = {"authorization": f"Bearer {client.cookies.get('forze_access')}"}
        client.cookies.clear()

        refused = client.post(_ACTIVATE.format(id=tenant_id), headers={**bearer, **_CROSS_SITE})

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"
        assert not refused.headers.get_list("set-cookie")

        allowed = client.post(_ACTIVATE.format(id=tenant_id), headers={**bearer, **_SAME_ORIGIN})

        assert allowed.status_code == 200
        assert None not in _session(client)

    def test_the_gate_runs_before_the_operation(self) -> None:
        # No middleware binds an identity: the gate answers before AuthnRequired would.
        _, client, tenant_id, _ = _session_app(_carrier(), middleware=False)

        refused = client.post(_ACTIVATE.format(id=tenant_id), headers=_CROSS_SITE)
        unauthenticated = client.post(_ACTIVATE.format(id=tenant_id), headers=_SAME_ORIGIN)

        assert refused.status_code == 403
        assert refused.headers[ERROR_CODE_HEADER] == "csrf_rejected"
        assert unauthenticated.status_code == 401

    def test_list_and_leave_set_no_cookie(self) -> None:
        _, client, tenant_id, _ = _session_app(_carrier())

        listed = client.get("/auth/tenants")
        left = client.delete(f"/auth/tenants/{tenant_id}", headers=_SAME_ORIGIN)

        assert listed.status_code == 200 and left.status_code == 204
        assert not listed.headers.get_list("set-cookie")
        assert not left.headers.get_list("set-cookie")

    def test_the_switch_route_needs_no_anonymous_path(self) -> None:
        # It requires a live access cookie; only login, refresh and logout must be listed.
        app, _, _, _ = _session_app(_carrier())

        check_authn_cookie_paths(app)

    def test_the_openapi_operation_matches_bearer_mode(self) -> None:
        cookie_app, _, _, _ = _session_app(_carrier())
        bearer_app, _, _, _ = _session_app(None)

        cookie_paths = cookie_app.openapi()["paths"]
        cookie_op = cookie_paths[_ACTIVATE]["post"]
        bearer_op = bearer_app.openapi()["paths"][_ACTIVATE]["post"]

        assert cookie_op == bearer_op
        # Documented as login documents its cookie-mode body: the token DTO, tokens optional.
        login_op = cookie_paths["/auth/login"]["post"]
        assert cookie_op["responses"]["200"] == login_op["responses"]["200"]

    def test_a_refused_switch_sets_no_cookie(self) -> None:
        _, client, _, _ = _session_app(_carrier())
        before = _session(client)

        refused = client.post(_ACTIVATE.format(id=uuid4()), headers=_SAME_ORIGIN)

        assert refused.status_code >= 400
        assert refused.headers[ERROR_CODE_HEADER] == "tenant_mismatch"
        assert not refused.headers.get_list("set-cookie")
        assert _session(client) == before

    @pytest.mark.parametrize(
        "override", ["/tenants/activate", "/tenants/{id}/{key}/activate"], ids=["drop", "add"]
    )
    def test_a_path_override_changing_the_placeholder_is_refused(self, override: str) -> None:
        with pytest.raises(CoreException, match="Path override"):
            attach_tenancy_routes(
                APIRouter(),
                registry=build_tenancy_registry(_SPEC).freeze(),
                ns=_NS,
                ctx_dep=lambda: None,
                path_overrides={TenancyKernelOp.SWITCH_TENANT: override},
                cookies=_carrier(),
            )

    def test_the_cookie_builder_declares_the_placeholders_it_fills(self) -> None:
        # The wrapper keeps id_endpoint's declaration, so a binding path naming another
        # placeholder is still refused instead of published with nothing behind it.
        binding = RouteBinding(
            method="POST",
            path="/tenants/{tenant}/activate",
            build=_cookie_token_endpoint(_carrier(), id_endpoint),
        )

        with pytest.raises(CoreException, match="does not fill"):
            attach_operation_routes(
                APIRouter(),
                registry=build_tenancy_registry(_SPEC).freeze(),
                ns=_NS,
                ctx_dep=lambda: None,
                bindings={TenancyKernelOp.SWITCH_TENANT: binding},
            )

    def test_the_cookie_builder_leaves_an_undeclared_builder_unchecked(self) -> None:
        # A builder of your own without the declaration is not checked, so the wrapper
        # must not invent one for it.
        def own_builder(runner: Any, input_type: Any, op: str) -> Any:
            return id_endpoint(runner, input_type, op)

        wrapped = _cookie_token_endpoint(_carrier(), own_builder)

        assert not hasattr(wrapped, PATH_PARAMS_ATTR)


class TestTenancyBearerMode:
    def test_switch_returns_the_tokens_in_the_body_and_sets_no_cookie(self) -> None:
        _, client, tenant_id, login = _session_app(None)
        bearer = {"authorization": f"Bearer {login['access_token']}"}

        response = client.post(_ACTIVATE.format(id=tenant_id), headers=bearer)

        assert response.status_code == 200
        body = response.json()
        assert body["access_token"] and body["refresh_token"]
        assert not response.headers.get_list("set-cookie")
