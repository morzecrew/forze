"""Unit tests for forze_fastapi.endpoints.document."""

from enum import StrEnum

import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.composition.document import (
    DocumentDTOs,
    build_document_registry,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import UsecasePlan
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.endpoints.document import attach_document_endpoints

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
        name="test",
        read=ReadDocument,
        write={
            "domain": domain,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": update_cmd,
        },
    )


def _minimal_spec_str_enum_name(
    supports_update: bool = False,
    supports_soft_delete: bool = False,
) -> DocumentSpec:
    """DocumentSpec with :class:`StrEnum` :attr:`name` (value ``\"test\"``)."""

    class DocName(StrEnum):
        TEST = "test"

    class UpdateCmd(BaseDTO):
        title: str | None = None

    update_cmd = UpdateCmd if supports_update else type("EmptyUpdate", (BaseDTO,), {})
    domain = _SoftDoc if supports_soft_delete else Document
    return DocumentSpec(
        name=DocName.TEST,
        read=ReadDocument,
        write={
            "domain": domain,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": update_cmd,
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
    """Build registry with plan merged and id set (required for attach_http_endpoint)."""
    reg = build_document_registry(spec, dtos).extend_plan(
        UsecasePlan().tx("*", route="mock")
    )
    reg.finalize(spec.name, inplace=True)
    return reg


def _metadata_endpoints() -> dict:
    """Map legacy /metadata path onto the document GET endpoint."""
    return {"get_": {"path_override": "/metadata"}}


class TestBuildDocumentRouter:
    """Tests for attaching document routes on an APIRouter (formerly build_document_router)."""

    def test_returns_router_with_metadata_route(
        self,
        composition_ctx,
    ) -> None:
        """attach_document_endpoints adds a GET route for document metadata/read."""
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/docs")
        attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints=_metadata_endpoints(),
        )

        assert isinstance(router, APIRouter)
        paths = {r.path for r in router.routes}
        assert "/docs/metadata" in paths or any("/metadata" in str(r) for r in router.routes)

    def test_str_enum_document_name_attaches_same_routes(
        self,
        composition_ctx,
    ) -> None:
        """``DocumentSpec.name`` may be a :class:`StrEnum`; routing matches string ``\"test\"``."""
        spec = _minimal_spec_str_enum_name()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/docs")
        attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints=_metadata_endpoints(),
        )

        paths = {r.path for r in router.routes}
        assert "/docs/metadata" in paths or any("/metadata" in str(r) for r in router.routes)

    def test_metadata_endpoint_invokes_get_usecase(
        self,
        composition_ctx,
    ) -> None:
        """GET /metadata invokes get usecase; mock raises NotFoundError for missing doc."""
        from uuid import uuid4

        from forze.base.errors import NotFoundError

        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/docs")
        attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints=_metadata_endpoints(),
        )

        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        pk = uuid4()
        with pytest.raises(NotFoundError, match="not found"):
            client.get(f"/docs/metadata?id={pk}")

    def test_metadata_endpoint_uses_composed_handler_not_plain_route_subclass(
        self,
        composition_ctx,
    ) -> None:
        """GET /metadata is registered; ETag is applied inside the HTTP handler pipeline."""
        from fastapi.routing import APIRoute

        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/docs")
        attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints=_metadata_endpoints(),
        )

        metadata_routes = [
            r
            for r in router.routes
            if hasattr(r, "path") and "/metadata" in getattr(r, "path", "")
        ]
        assert len(metadata_routes) == 1
        route = metadata_routes[0]
        assert isinstance(route, APIRoute)

    def test_respects_path_overrides(
        self,
        composition_ctx,
    ) -> None:
        """Path overrides apply per endpoint via DocumentEndpointsSpec."""
        spec = _minimal_spec(supports_update=True, supports_soft_delete=True)
        dtos = _minimal_dtos(supports_update=True)
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/docs")
        attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                "get_": {"path_override": "/meta"},
                "list_": {"path_override": "/query"},
                "raw_list": {"path_override": "/raw-query"},
                "list_cursor": {"path_override": "/query-cursor"},
                "raw_list_cursor": {"path_override": "/raw-query-cursor"},
                "create": {"path_override": "/new"},
                "update": {"path_override": "/edit"},
                "delete": {"path_override": "/archive"},
                "restore": {"path_override": "/unarchive"},
                "kill": {"path_override": "/purge"},
            },
        )

        paths = {r.path for r in router.routes}
        assert any(path.endswith("/meta") for path in paths)
        assert any(path.endswith("/query") for path in paths)
        assert any(path.endswith("/raw-query") for path in paths)
        assert any(path.endswith("/query-cursor") for path in paths)
        assert any(path.endswith("/raw-query-cursor") for path in paths)
        assert any(path.endswith("/new") for path in paths)
        assert any(path.endswith("/edit") for path in paths)
        assert any(path.endswith("/archive") for path in paths)
        assert any(path.endswith("/unarchive") for path in paths)
        assert any(path.endswith("/purge") for path in paths)


class TestAttachDocumentRoutes:
    """Tests for attach_document_endpoints."""

    def test_attach_adds_routes_to_existing_router(
        self,
        composition_ctx,
    ) -> None:
        """attach_document_endpoints adds document endpoints to an existing router."""
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/api")
        result = attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints=_metadata_endpoints(),
        )

        assert result is router
        paths = {r.path for r in router.routes}
        assert "/api/metadata" in paths or any("/metadata" in str(r) for r in router.routes)

    def test_can_enable_list_and_raw_list_endpoints(
        self,
        composition_ctx,
    ) -> None:
        """List and raw-list routes are registered when enabled in the endpoints spec."""
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/api")
        attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                **_metadata_endpoints(),
                "list_": True,
                "raw_list": True,
            },
        )

        paths = {r.path for r in router.routes}
        assert any(path.endswith("/list") for path in paths)
        assert any(path.endswith("/raw-list") for path in paths)

    def test_can_enable_list_cursor_endpoints(
        self,
        composition_ctx,
    ) -> None:
        """List cursor and raw list cursor routes register when enabled."""
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/api")
        attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                **_metadata_endpoints(),
                "list_cursor": True,
                "raw_list_cursor": True,
            },
        )

        paths = {r.path for r in router.routes}
        assert any(path.endswith("/list-cursor") for path in paths)
        assert any(path.endswith("/raw-list-cursor") for path in paths)

    def test_can_disable_metadata_and_write_related_endpoints(
        self,
        composition_ctx,
    ) -> None:
        """Endpoint flags can skip GET metadata and all write endpoints."""
        spec = _minimal_spec(supports_update=True, supports_soft_delete=True)
        dtos = _minimal_dtos(supports_update=True)
        reg = _build_registry(spec, dtos)

        def ctx_dep():
            return composition_ctx

        router = APIRouter(prefix="/api")
        attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=ctx_dep,
            endpoints={
                "get_": False,
                "create": False,
                "update": False,
                "delete": False,
                "restore": False,
                "kill": False,
            },
        )

        paths = {r.path for r in router.routes}
        assert all(not path.endswith("/metadata") for path in paths)
        assert all(not path.endswith("/get") for path in paths)
        assert all(not path.endswith("/create") for path in paths)
        assert all(not path.endswith("/update") for path in paths)
        assert all(not path.endswith("/delete") for path in paths)
        assert all(not path.endswith("/restore") for path in paths)
        assert all(not path.endswith("/kill") for path in paths)
