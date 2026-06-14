"""Route-surface tests for ``attach_tenancy_routes`` (the tenant selector)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, FastAPI

from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.routes import attach_tenancy_routes
from forze_kits.aggregates.tenancy import TenancyKernelOp, build_tenancy_registry

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
    def test_routes_are_list_and_activate(self) -> None:
        paths = _build_app().openapi()["paths"]

        assert set(paths) == {"/auth/tenants", "/auth/tenants/{id}/activate"}
        assert set(paths["/auth/tenants"]) == {"get"}
        assert set(paths["/auth/tenants/{id}/activate"]) == {"post"}

    def test_operation_ids_are_registry_keys_verbatim(self) -> None:
        assert _op_ids(_build_app()) == {f"main.{op.value}" for op in TenancyKernelOp}

    def test_both_routes_flagged_requires_authn(self) -> None:
        paths = _build_app().openapi()["paths"]

        for methods in paths.values():
            for meta in methods.values():
                assert meta.get("x-requires-authn") is True

    def test_include_narrows_to_subset(self) -> None:
        app = _build_app(include={TenancyKernelOp.LIST_TENANTS})

        assert set(app.openapi()["paths"]) == {"/auth/tenants"}
