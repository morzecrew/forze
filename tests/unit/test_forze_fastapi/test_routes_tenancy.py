"""Route-surface tests for ``attach_tenancy_routes`` (the tenant selector)."""

from __future__ import annotations

import asyncio
import secrets
from typing import Any
from uuid import UUID, uuid4

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
from forze.domain.models import ReadDocument
from forze_fastapi.exceptions import ERROR_CODE_HEADER, register_exception_handlers
from forze_fastapi.middlewares import SecurityContextMiddleware
from forze_fastapi.routes import attach_authn_routes, attach_tenancy_routes
from forze_fastapi.security import AuthnRequirement, HeaderTokenAuthn
from forze_identity.tenancy import TenancyDepsModule, principal_tenant_binding_spec, tenant_spec
from forze_kits.aggregates.authn import build_authn_registry
from forze_kits.aggregates.tenancy import TenancyKernelOp, build_tenancy_registry
from forze_mock import MockDepsModule
from forze_mock.execution.configs import MockRouteConfig
from forze_mock.execution.factories import ConfigurableMockDocument
from tests.support.authn_stacks import AUTHN_ROUTE, identity_authn_context, stack_principal

# ----------------------- #

_SPEC = AuthnSpec(name="main", enabled_methods=frozenset({"token"}))
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
