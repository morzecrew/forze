"""Route-surface tests for ``attach_tenancy_admin_routes`` (privileged tenant admin)."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, FastAPI

from forze.application.contracts.authn import AuthnSpec
from forze_fastapi.routes import attach_tenancy_admin_routes
from forze_kits.aggregates.tenancy_admin import (
    TenancyAdminKernelOp,
    build_tenancy_admin_registry,
)

# ----------------------- #

_NS = AuthnSpec(name="main", enabled_methods=frozenset({"token"})).default_namespace


def _build_app(*, include: Any = None) -> FastAPI:
    reg = build_tenancy_admin_registry(_NS).freeze()
    router = APIRouter(prefix="/admin")
    attach_tenancy_admin_routes(
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


class TestTenancyAdminRouteSurface:
    def test_route_surface(self) -> None:
        paths = _build_app().openapi()["paths"]

        assert set(paths) == {
            "/admin/tenants",
            "/admin/tenants/{id}/members",
            "/admin/tenants/{id}/deactivate",
            "/admin/memberships",
        }
        assert set(paths["/admin/tenants"]) == {"post"}
        assert set(paths["/admin/tenants/{id}/members"]) == {"get"}
        assert set(paths["/admin/tenants/{id}/deactivate"]) == {"post"}
        assert set(paths["/admin/memberships"]) == {"post", "delete"}

    def test_operation_ids_are_registry_keys_verbatim(self) -> None:
        assert _op_ids(_build_app()) == {
            f"main.{op.value}" for op in TenancyAdminKernelOp
        }

    def test_ships_unguarded_no_requires_authn_flag(self) -> None:
        # Admin ops are guarded by the app, not the framework — none are pre-flagged.
        paths = _build_app().openapi()["paths"]

        for methods in paths.values():
            for meta in methods.values():
                assert meta.get("x-requires-authn") is not True

    def test_include_narrows_to_subset(self) -> None:
        app = _build_app(include={TenancyAdminKernelOp.CREATE_TENANT})

        assert set(app.openapi()["paths"]) == {"/admin/tenants"}
