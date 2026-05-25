"""Unit tests for ``forze_fastapi.transport.http.attach_document_routes``."""

from __future__ import annotations

from enum import StrEnum
from uuid import uuid4

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute
from starlette.testclient import TestClient

from forze.application.composition.document import (
    DocumentDTOs,
    DocumentFacade,
    build_document_registry,
)
from forze.base.errors import NotFoundError
from forze_contrib.soft_deletion import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.transport.http import attach_document_routes, make_facade_dep
from registry_helpers import freeze_registry

from forze.application.contracts.document import DocumentSpec

pytestmark = pytest.mark.unit


class _SoftDoc(Document, SoftDeletionMixin):
    pass


def _minimal_spec(
    *,
    supports_update: bool = False,
    supports_soft_delete: bool = False,
) -> DocumentSpec:
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


def _minimal_dtos(*, supports_update: bool = False) -> DocumentDTOs:
    class UpdateCmd(BaseDTO):
        title: str | None = None

    empty_update = type("EmptyUpdate", (BaseDTO,), {})
    return DocumentDTOs(
        read=ReadDocument,
        create=CreateDocumentCmd,
        update=UpdateCmd if supports_update else empty_update,
    )


def _attach(
    router: APIRouter,
    spec: DocumentSpec,
    dtos: DocumentDTOs,
    composition_ctx,
    *,
    enable: tuple[str, ...] = ("get",),
    paths: dict[str, str] | None = None,
    per_route: dict | None = None,
    config: dict | None = None,
) -> APIRouter:
    reg = freeze_registry(build_document_registry(spec, dtos))

    def ctx_dep():
        return composition_ctx

    facade_dep = make_facade_dep(
        DocumentFacade,
        registry=reg,
        namespace=spec.default_namespace,
        ctx_dep=ctx_dep,
    )
    return attach_document_routes(
        router,
        document=spec,
        dtos=dtos,
        facade_dep=facade_dep,
        ctx_dep=ctx_dep,
        registry=reg,
        enable=enable,
        paths=paths,
        per_route=per_route,
        config=config,
    )


class TestAttachDocumentRoutes:
    def test_metadata_path_override(self, composition_ctx) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        router = APIRouter(prefix="/docs")
        _attach(
            router,
            spec,
            dtos,
            composition_ctx,
            enable=("get",),
            per_route={"get": {"path_override": "/metadata"}},
        )
        paths = {r.path for r in router.routes}
        assert any("/metadata" in path for path in paths)

    def test_get_invokes_usecase(self, composition_ctx) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        router = APIRouter(prefix="/docs")
        _attach(
            router,
            spec,
            dtos,
            composition_ctx,
            enable=("get",),
            per_route={"get": {"path_override": "/metadata"}},
        )
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)
        pk = uuid4()
        with pytest.raises(NotFoundError, match="not found"):
            client.get(f"/docs/metadata?id={pk}")

    def test_path_overrides(self, composition_ctx) -> None:
        spec = _minimal_spec(supports_update=True, supports_soft_delete=True)
        dtos = _minimal_dtos(supports_update=True)
        router = APIRouter(prefix="/docs")
        _attach(
            router,
            spec,
            dtos,
            composition_ctx,
            enable=(
                "get",
                "list",
                "raw_list",
                "list_cursor",
                "raw_list_cursor",
                "create",
                "update",
                "delete",
                "restore",
                "kill",
            ),
            per_route={
                "get": {"path_override": "/meta"},
                "list": {"path_override": "/query"},
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
        for suffix in (
            "/meta",
            "/query",
            "/raw-query",
            "/query-cursor",
            "/raw-query-cursor",
            "/new",
            "/edit",
            "/archive",
            "/unarchive",
            "/purge",
        ):
            assert any(path.endswith(suffix) for path in paths)

    def test_str_enum_document_name(self, composition_ctx) -> None:
        class DocName(StrEnum):
            TEST = "test"

        spec = DocumentSpec(
            name=DocName.TEST,
            read=ReadDocument,
            write={
                "domain": Document,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": type("EmptyUpdate", (BaseDTO,), {}),
            },
        )
        dtos = _minimal_dtos()
        router = APIRouter(prefix="/docs")
        _attach(
            router,
            spec,
            dtos,
            composition_ctx,
            enable=("get",),
            per_route={"get": {"path_override": "/metadata"}},
        )
        assert router.routes

    def test_routes_are_api_route_instances(self, composition_ctx) -> None:
        spec = _minimal_spec()
        dtos = _minimal_dtos()
        router = APIRouter(prefix="/docs")
        _attach(
            router,
            spec,
            dtos,
            composition_ctx,
            enable=("get",),
            per_route={"get": {"path_override": "/metadata"}},
        )
        metadata_routes = [
            r for r in router.routes if hasattr(r, "path") and "/metadata" in getattr(r, "path", "")
        ]
        assert len(metadata_routes) == 1
        assert isinstance(metadata_routes[0], APIRoute)
