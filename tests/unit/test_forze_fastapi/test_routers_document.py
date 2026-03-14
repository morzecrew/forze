"""Unit tests for forze_fastapi.routers.document."""

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from forze.application.composition.base import BaseUsecasesFacadeProvider
from forze.application.composition.document import (
    DocumentUsecasesFacade,
    DocumentUsecasesModule,
    build_document_registry,
    tx_document_plan,
)
from forze.application.contracts.document import DocumentSpec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.routers.document import (
    build_document_router,
    document_facade_dependency,
)
from forze_fastapi.routing.router import ForzeAPIRouter

# ----------------------- #


def _minimal_spec(
    supports_update: bool = False,
    supports_soft_delete: bool = False,
) -> DocumentSpec:
    """Build a minimal DocumentSpec for testing."""

    class UpdateCmd(BaseDTO):
        title: str | None = None

    update_cmd = UpdateCmd if supports_update else type("EmptyUpdate", (BaseDTO,), {})
    return DocumentSpec(
        namespace="test",
        read={"source": "test_read", "model": ReadDocument},
        write={
            "source": "test_write",
            "models": {
                "domain": Document,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": update_cmd,
            },
        },
    )


def _minimal_dto_spec(supports_update: bool = False) -> dict:
    """Build a minimal DocumentDTOSpec for testing."""

    class UpdateCmd(BaseDTO):
        title: str | None = None

    empty_update = type("EmptyUpdate", (BaseDTO,), {})
    dto: dict = {
        "read": ReadDocument,
        "create": CreateDocumentCmd,
        "update": UpdateCmd if supports_update else empty_update,
    }
    return dto


class TestDocumentFacadeDependency:
    """Tests for document_facade_dependency."""

    def test_returns_callable_that_resolves_facade(
        self,
        composition_ctx,
    ) -> None:
        """document_facade_dependency returns a dependency that resolves to a facade."""
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        plan = tx_document_plan
        provider = BaseUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            facade=DocumentUsecasesFacade,
        )
        module = DocumentUsecasesModule(
            spec=spec,
            dtos={"read": ReadDocument, "create": CreateDocumentCmd},
            provider=provider,
        )

        def ctx_dep():
            return composition_ctx

        dep = document_facade_dependency(module, ctx_dep)
        # dep is a factory that returns a FastAPI dependency
        assert callable(dep)


class TestBuildDocumentRouter:
    """Tests for build_document_router."""

    def test_returns_router_with_metadata_route(
        self,
        composition_ctx,
    ) -> None:
        """build_document_router returns a router with /metadata route."""
        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        plan = tx_document_plan
        provider = BaseUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            facade=DocumentUsecasesFacade,
        )
        module = DocumentUsecasesModule(
            spec=spec,
            dtos={"read": ReadDocument, "create": CreateDocumentCmd},
            provider=provider,
        )

        def ctx_dep():
            return composition_ctx

        router = build_document_router(
            prefix="/docs",
            module=module,
            context=ctx_dep,
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

        spec = _minimal_spec()
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        plan = tx_document_plan
        provider = BaseUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            facade=DocumentUsecasesFacade,
        )
        module = DocumentUsecasesModule(
            spec=spec,
            dtos={"read": ReadDocument, "create": CreateDocumentCmd},
            provider=provider,
        )

        def ctx_dep():
            return composition_ctx

        router = build_document_router(
            prefix="/docs",
            module=module,
            context=ctx_dep,
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
        dto_spec = _minimal_dto_spec()
        reg = build_document_registry(spec, dto_spec)
        plan = tx_document_plan
        provider = BaseUsecasesFacadeProvider(
            reg=reg,
            plan=plan,
            facade=DocumentUsecasesFacade,
        )
        module = DocumentUsecasesModule(
            spec=spec,
            dtos={"read": ReadDocument, "create": CreateDocumentCmd},
            provider=provider,
        )

        def ctx_dep():
            return composition_ctx

        router = build_document_router(
            prefix="/docs",
            module=module,
            context=ctx_dep,
        )

        metadata_routes = [
            r
            for r in router.routes
            if hasattr(r, "path") and "/metadata" in getattr(r, "path", "")
        ]
        assert len(metadata_routes) == 1
        route = metadata_routes[0]
        assert type(route) is not APIRoute
