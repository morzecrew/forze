"""Unit tests for forze_fastapi.routers.search."""

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.testclient import TestClient

from forze.application.composition.search import (
    SearchUsecasesFacadeProvider,
    build_search_plan,
    build_search_registry,
)
from forze.application.contracts.search import SearchSpec
from forze_fastapi.routers.search import (
    attach_search_routes,
    build_search_router,
    search_facade_dependency,
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


class TestSearchFacadeDependency:
    """Tests for search_facade_dependency."""

    def test_returns_callable(
        self,
        composition_ctx,
    ) -> None:
        """search_facade_dependency returns a callable dependency factory."""
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        plan = build_search_plan()
        provider = SearchUsecasesFacadeProvider(
            spec=spec,
            read_dto=ReadDTO,
            reg=reg,
            plan=plan,
        )

        def ctx_dep():
            return composition_ctx

        dep = search_facade_dependency(provider, ctx_dep)
        assert callable(dep)


class TestAttachSearchRouter:
    """Tests for attach_search_router."""

    def test_adds_search_routes(
        self,
        composition_ctx,
    ) -> None:
        """attach_search_router adds /search and /raw-search routes."""
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        plan = build_search_plan()
        provider = SearchUsecasesFacadeProvider(
            spec=spec,
            read_dto=ReadDTO,
            reg=reg,
            plan=plan,
        )

        def ctx_dep():
            return composition_ctx

        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=ctx_dep,
        )
        result = attach_search_routes(router, provider=provider, context=ctx_dep)

        assert result is router
        paths = {r.path for r in router.routes}
        assert "/api/search" in paths
        assert "/api/raw-search" in paths

    def test_search_endpoint_returns_paginated(
        self,
        composition_ctx,
    ) -> None:
        """POST /search returns paginated results."""
        spec = _minimal_search_spec()
        reg = build_search_registry(spec)
        plan = build_search_plan()
        provider = SearchUsecasesFacadeProvider(
            spec=spec,
            read_dto=ReadDTO,
            reg=reg,
            plan=plan,
        )

        def ctx_dep():
            return composition_ctx

        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=ctx_dep,
        )
        attach_search_routes(router, provider=provider, context=ctx_dep)

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
        reg = build_search_registry(spec)
        plan = build_search_plan()
        provider = SearchUsecasesFacadeProvider(
            spec=spec,
            read_dto=ReadDTO,
            reg=reg,
            plan=plan,
        )

        def ctx_dep():
            return composition_ctx

        router = build_search_router(
            prefix="/search",
            provider=provider,
            context=ctx_dep,
        )

        assert isinstance(router, ForzeAPIRouter)
        paths = {r.path for r in router.routes}
        assert "/search/search" in paths or "/search" in paths
        assert "/search/raw-search" in paths or "/raw-search" in paths
