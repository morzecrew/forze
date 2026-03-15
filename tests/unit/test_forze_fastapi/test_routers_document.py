"""Unit tests for forze_fastapi.routers.document."""

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from forze.application.composition.document import (
    DocumentDTOs,
    build_document_registry,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import UsecasePlan
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.routers.document import (
    attach_document_routes,
    build_document_router,
)
from forze_fastapi.routing.router import ForzeAPIRouter

# ----------------------- #


class _SoftDoc(Document, SoftDeletionMixin):
    """Document with soft-delete for tests."""

    pass


def _minimal_spec(
    supports_update: bool = False,
    supports_soft_delete: bool = False,
) -> DocumentSpec:
    """Build a minimal DocumentSpec for testing."""

    class UpdateCmd(BaseDTO):
        title: str | None = None

    update_cmd = UpdateCmd if supports_update else type("EmptyUpdate", (BaseDTO,), {})
    domain = _SoftDoc if supports_soft_delete else Document
    return DocumentSpec(
        namespace="test",
        read={"source": "test_read", "model": ReadDocument},
        write={
            "source": "test_write",
            "models": {
                "domain": domain,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": update_cmd,
            },
        },
    )


def _minimal_dtos(supports_update: bool = False) -> DocumentDTOs:
    """Build minimal DocumentDTOs for testing."""

    class UpdateCmd(BaseDTO):
        title: str | None = None

    empty_update = type("EmptyUpdate", (BaseDTO,), {})
    return DocumentDTOs(
        read=ReadDocument,
        create=CreateDocumentCmd,
        update=UpdateCmd if supports_update else empty_update,
    )


def _build_registry(spec: DocumentSpec, dtos: DocumentDTOs):
    """Build registry with plan merged."""
    reg = build_document_registry(spec, dtos)
    return reg.extend_plan(UsecasePlan().tx("*"))


class TestBuildDocumentRouter:
    """Tests for build_document_router."""

    def test_returns_router_with_metadata_route(
        self,
        composition_ctx,
    ) -> None:
        """build_document_router returns a router with /metadata route."""
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = build_document_router(
            prefix="/docs",
            registry=reg,
            spec=spec,
            dtos=dtos,
            ctx_dep=ctx_dep,
        )

        assert isinstance(router, ForzeAPIRouter)
        paths = {r.path for r in router.routes}
        assert "/metadata" in paths or any("/metadata" in str(r) for r in router.routes)

    def test_metadata_endpoint_invokes_get_usecase(
        self,
        composition_ctx,
    ) -> None:
        """GET /metadata invokes get usecase; mock raises NotFoundError for missing doc."""
        from uuid import uuid4

        from forze.base.errors import NotFoundError

        spec = _minimal_spec(supports_update=True, supports_soft_delete=True)
        dtos = _minimal_dtos(supports_update=True)
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = build_document_router(
            prefix="/docs",
            registry=reg,
            spec=spec,
            dtos=dtos,
            ctx_dep=ctx_dep,
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        pk = uuid4()
        with pytest.raises(NotFoundError, match="not found"):
            client.get(f"/docs/metadata?id={pk}")

    def test_metadata_endpoint_uses_etag_feature(
        self,
        composition_ctx,
    ) -> None:
        """GET /metadata route uses composed route class with ETag feature."""
        from fastapi.routing import APIRoute

        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = build_document_router(
            prefix="/docs",
            registry=reg,
            spec=spec,
            dtos=dtos,
            ctx_dep=ctx_dep,
        )

        metadata_routes = [
            r
            for r in router.routes
            if hasattr(r, "path") and "/metadata" in getattr(r, "path", "")
        ]
        assert len(metadata_routes) == 1
        route = metadata_routes[0]
        assert type(route) is not APIRoute


class TestAttachDocumentRoutes:
    """Tests for attach_document_routes."""

    def test_attach_adds_routes_to_existing_router(
        self,
        composition_ctx,
    ) -> None:
        """attach_document_routes adds document endpoints to an existing router."""
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = ForzeAPIRouter(
            prefix="/api",
            context_dependency=ctx_dep,
        )
        result = attach_document_routes(
            router,
            registry=reg,
            spec=spec,
            dtos=dtos,
            ctx_dep=ctx_dep,
        )

        assert result is router
        paths = {r.path for r in router.routes}
        assert "/metadata" in paths or any("/metadata" in str(r) for r in router.routes)
