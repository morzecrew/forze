"""Unit tests for forze_fastapi.routers.search."""

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.testclient import TestClient

from forze.application.composition.search import (
    SearchDTOs,
    build_search_registry,
)
from forze.application.contracts.search import SearchSpec
from forze_fastapi.routers.search import (
    attach_search_routes,
    build_search_router,
)
from forze_fastapi.routing.router import ForzeAPIRouter

# ----------------------- #


class ReadDTO(BaseModel):
    """Minimal read DTO for search tests."""

    id: str
    title: str


def _minimal_search_spec() -> SearchSpec[ReadDTO]:
    """Build a minimal SearchSpec for testing."""
    return SearchSpec(
        namespace="test_search",
        model=ReadDTO,
        indexes={"default": {"fields": [{"path": "title"}]}},
    )


def _minimal_search_dtos() -> SearchDTOs:
    """Build minimal SearchDTOs for testing."""
    return SearchDTOs(read=ReadDTO)


class TestAttachSearchRoutes:
    """Tests for attach_search_router."""

    def test_adds_search_routes(
        self,
        composition_ctx,
    ) -> None:
        """attach_search_routes adds /search and /raw-search routes."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=ctx_dep,
        )
        result = attach_search_routes(
            router,
            registry=reg,
            spec=spec,
            dtos=dtos,
            ctx_dep=ctx_dep,
        )

        assert result is router
        paths = {r.path for r in router.routes}
        assert "/search" in paths or "/api/search" in paths
        assert "/raw-search" in paths or "/api/raw-search" in paths

    def test_search_endpoint_returns_paginated(
        self,
        composition_ctx,
    ) -> None:
        """POST /search returns paginated results."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=ctx_dep,
        )
        attach_search_routes(
            router,
            registry=reg,
            spec=spec,
            dtos=dtos,
            ctx_dep=ctx_dep,
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


class TestBuildSearchRouter:
    """Tests for build_search_router."""

    def test_returns_router_with_search_routes(
        self,
        composition_ctx,
    ) -> None:
        """build_search_router returns a router with search routes."""
        spec = _minimal_search_spec()
        dtos = _minimal_search_dtos()
        reg = build_search_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = build_search_router(
            prefix="/search",
            registry=reg,
            spec=spec,
            dtos=dtos,
            ctx_dep=ctx_dep,
        )

        assert isinstance(router, ForzeAPIRouter)
        paths = {r.path for r in router.routes}
        assert "/search/search" in paths or "/search" in paths
        assert "/search/raw-search" in paths or "/raw-search" in paths
