"""Unit tests for forze_fastapi.endpoints.search."""

from fastapi import APIRouter, FastAPI
from pydantic import BaseModel
from starlette.testclient import TestClient

from forze.application.composition.search import (
    SearchDTOs,
    build_search_registry,
)
from forze.application.contracts.search import SearchSpec
from forze_fastapi.endpoints.http import AuthnRequirement
from forze_fastapi.endpoints.search import attach_search_endpoints

# ----------------------- #


class ReadDTO(BaseModel):
    """Minimal read DTO for search tests."""

    id: str
    title: str


def _minimal_search_spec() -> SearchSpec[ReadDTO]:
    """Build a minimal SearchSpec for testing."""
    return SearchSpec(
        name="test_search",
        model_type=ReadDTO,
        fields=["title"],
    )


def _minimal_search_dtos() -> SearchDTOs:
    """Build minimal SearchDTOs for testing."""
    return SearchDTOs(read=ReadDTO)


class TestAttachSearchRoutes:
    """Tests for attach_search_endpoints."""

    def test_adds_search_routes(
        self,
        composition_ctx,
    ) -> None:
        """attach_search_endpoints adds /search and /raw-search routes."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec)
        reg.finalize(spec.name, inplace=True)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/api")
        result = attach_search_endpoints(
            router,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                "search": True,
                "raw_search": True,
                "search_cursor": True,
                "raw_search_cursor": True,
            },
        )

        assert result is router
        paths = {r.path for r in router.routes}
        assert "/search" in paths or "/api/search" in paths
        assert "/raw-search" in paths or "/api/raw-search" in paths
        assert "/search-cursor" in paths or "/api/search-cursor" in paths
        assert "/raw-search-cursor" in paths or "/api/raw-search-cursor" in paths

    def test_search_endpoint_returns_paginated(
        self,
        composition_ctx,
    ) -> None:
        """POST /search returns paginated results."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec)
        reg.finalize(spec.name, inplace=True)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/api")
        attach_search_endpoints(
            router,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                "search": True,
                "raw_search": True,
                "search_cursor": True,
                "raw_search_cursor": True,
            },
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        response = client.post(
            "/api/search",
            json={"query": ""},
        )
        assert response.status_code == 200
        data = response.json()
        assert "hits" in data or "items" in data
        assert "count" in data or "total" in data

    def test_base_authn_requirement_applies_to_all_endpoints(
        self,
        composition_ctx,
    ) -> None:
        """``endpoints['authn']`` enforces 401 on every produced search route."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec)
        reg.finalize(spec.name, inplace=True)

        def ctx_dep():
            return composition_ctx

        base_req = AuthnRequirement(
            authn_route="api",
            token_header="Authorization",
        )

        router = APIRouter(prefix="/api")
        attach_search_endpoints(
            router,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                "search": True,
                "raw_search": True,
                "authn": base_req,
            },
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        assert client.post("/api/search", json={"query": ""}).status_code == 401
        assert (
            client.post(
                "/api/raw-search",
                json={"query": "", "return_fields": ["title"]},
            ).status_code
            == 401
        )

        openapi = app.openapi()
        paths = openapi["paths"]
        for path in ("/api/search", "/api/raw-search"):
            op = paths[path]["post"]
            assert any(base_req.scheme_name in entry for entry in op["security"])
            assert base_req.scheme_name in op["components"]["securitySchemes"]

    def test_per_endpoint_authn_overrides_base(
        self,
        composition_ctx,
    ) -> None:
        """Per-endpoint ``authn`` overrides the base on the matching route only."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec)
        reg.finalize(spec.name, inplace=True)

        def ctx_dep():
            return composition_ctx

        base_req = AuthnRequirement(
            authn_route="api",
            token_header="Authorization",
        )
        override_req = AuthnRequirement(
            authn_route="api",
            api_key_header="X-API-Key",
        )

        router = APIRouter(prefix="/api")
        attach_search_endpoints(
            router,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                "search": True,
                "raw_search": {"authn": override_req},
                "authn": base_req,
            },
        )

        app = FastAPI()
        app.include_router(router)
        openapi = app.openapi()

        paths = openapi["paths"]
        search_security = paths["/api/search"]["post"]["security"]
        raw_security = paths["/api/raw-search"]["post"]["security"]

        assert any(base_req.scheme_name in entry for entry in search_security)
        assert any(override_req.scheme_name in entry for entry in raw_security)
        assert all(base_req.scheme_name not in entry for entry in raw_security)

    def test_can_disable_typed_search_endpoint(
        self,
        composition_ctx,
    ) -> None:
        """Typed search can be disabled while raw search remains."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec)
        reg.finalize(spec.name, inplace=True)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/api")
        attach_search_endpoints(
            router,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                "search": False,
                "raw_search": True,
            },
        )

        paths = {r.path for r in router.routes}
        assert all(not path.endswith("/search") for path in paths)
        assert any(path.endswith("/raw-search") for path in paths)


class TestBuildSearchRouter:
    """Tests for composing a prefixed router with attach_search_endpoints."""

    def test_returns_router_with_search_routes(
        self,
        composition_ctx,
    ) -> None:
        """Router under /search exposes nested /search and /raw-search paths."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec)
        reg.finalize(spec.name, inplace=True)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/search")
        attach_search_endpoints(
            router,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                "search": True,
                "raw_search": True,
                "search_cursor": True,
                "raw_search_cursor": True,
            },
        )

        assert isinstance(router, APIRouter)
        paths = {r.path for r in router.routes}
        assert "/search/search" in paths or "/search" in paths
        assert "/search/raw-search" in paths or "/raw-search" in paths
        assert any(
            p.endswith("/search-cursor") or p.endswith("/search/search-cursor")
            for p in paths
        )
        assert any(
            p.endswith("/raw-search-cursor") or p.endswith("/search/raw-search-cursor")
            for p in paths
        )

    def test_respects_endpoint_flags_and_path_overrides(
        self,
        composition_ctx,
    ) -> None:
        """SearchEndpointsSpec path_override and disable flags apply."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec)
        reg.finalize(spec.name, inplace=True)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/search")
        attach_search_endpoints(
            router,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                "search": {"path_override": "/query"},
                "raw_search": False,
            },
        )

        paths = {r.path for r in router.routes}
        assert any(path.endswith("/query") for path in paths)
        assert all(not path.endswith("/raw-search") for path in paths)
