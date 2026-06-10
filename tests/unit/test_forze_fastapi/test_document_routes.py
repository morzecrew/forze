"""Tests for generated document routes (forze_fastapi.routes.attach_document_routes)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, ReadDocument
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_document_routes
from forze_kits.aggregates.document import (
    DocumentDTOs,
    DocumentKernelOp,
    build_document_registry,
)
from forze_kits.aggregates.soft_deletion import (
    SoftDeletionKernelOp,
    build_soft_deletion_registry,
)
from forze_kits.domain.soft_deletion.models import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _NoteRead(ReadDocument):
    title: str
    is_deleted: bool = False


class _NoteCreate(BaseDTO):
    title: str = ""


class _NoteUpdate(UpdateCmdWithSoftDeletion):
    title: str | None = None


class _Note(DocWithSoftDeletion):
    title: str = ""


# ....................... #


def _spec(*, writable: bool = True) -> DocumentSpec[_NoteRead, _Note, _NoteCreate, _NoteUpdate]:
    return DocumentSpec(
        name="notes",
        read=_NoteRead,
        write=(
            DocumentWriteTypes(
                domain=_Note, create_cmd=_NoteCreate, update_cmd=_NoteUpdate
            )
            if writable
            else None
        ),
    )


def _registry(spec: DocumentSpec) -> FrozenOperationRegistry:
    dtos = DocumentDTOs(
        read=_NoteRead,
        create=_NoteCreate if spec.write else None,
        update=_NoteUpdate if spec.write else None,
    )
    reg = build_document_registry(spec, dtos)
    reg = reg.merge(build_soft_deletion_registry(spec))
    return reg.freeze()


def _build_app(
    style,
    *,
    writable: bool = True,
    include=None,
) -> FastAPI:
    spec = _spec(writable=writable)
    state = MockState()

    router = APIRouter(prefix="/notes")
    attach_document_routes(
        router,
        registry=_registry(spec),
        ns=spec.default_namespace,
        ctx_dep=lambda: context_from_modules(MockDepsModule(state=state)),
        style=style,
        include=include,
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)

    return app


def _operation_ids(app: FastAPI) -> set[str]:
    return {
        operation["operationId"]
        for methods in app.openapi()["paths"].values()
        for operation in methods.values()
    }


_ALL_OPS = (*DocumentKernelOp, *SoftDeletionKernelOp)
_ALL_OP_IDS = {f"notes.{op.value}" for op in _ALL_OPS}
_READ_OP_IDS = {
    f"notes.{op.value}"
    for op in (
        DocumentKernelOp.GET,
        DocumentKernelOp.LIST,
        DocumentKernelOp.RAW_LIST,
        DocumentKernelOp.LIST_CURSOR,
        DocumentKernelOp.RAW_LIST_CURSOR,
        DocumentKernelOp.AGG_LIST,
    )
}


# ....................... #


class TestRestStyle:
    def test_crud_round_trip(self) -> None:
        client = TestClient(_build_app("rest"))

        created = client.post("/notes", json={"title": "hello"})
        assert created.status_code == 201
        note = created.json()
        assert note["title"] == "hello"

        fetched = client.get(f"/notes/{note['id']}")
        assert fetched.status_code == 200
        assert fetched.json()["title"] == "hello"

        updated = client.patch(
            f"/notes/{note['id']}",
            params={"rev": note["rev"]},
            json={"title": "world"},
        )
        assert updated.status_code == 200
        assert updated.json()["data"]["title"] == "world"

        listed = client.post("/notes/list", json={})
        assert listed.status_code == 200
        assert [hit["title"] for hit in listed.json()["hits"]] == ["world"]

        killed = client.delete(f"/notes/{note['id']}")
        assert killed.status_code == 204

        assert client.get(f"/notes/{note['id']}").status_code == 404

    def test_paths_and_methods(self) -> None:
        paths = _build_app("rest").openapi()["paths"]

        assert set(paths["/notes"]) == {"post"}
        assert set(paths["/notes/{id}"]) == {"get", "patch", "delete"}
        assert set(paths["/notes/list"]) == {"post"}
        assert set(paths["/notes/agg_list"]) == {"post"}
        assert set(paths["/notes/{id}/delete"]) == {"post"}
        assert set(paths["/notes/{id}/restore"]) == {"post"}

    def test_soft_delete_and_restore(self) -> None:
        client = TestClient(_build_app("rest"))

        note = client.post("/notes", json={"title": "x"}).json()

        deleted = client.post(
            f"/notes/{note['id']}/delete", params={"rev": note["rev"]}
        )
        assert deleted.status_code == 200
        assert deleted.json()["is_deleted"] is True

        restored = client.post(
            f"/notes/{note['id']}/restore",
            params={"rev": deleted.json()["rev"]},
        )
        assert restored.status_code == 200
        assert restored.json()["is_deleted"] is False

    def test_update_rev_is_a_required_query_param(self) -> None:
        client = TestClient(_build_app("rest"))

        created = client.post("/notes", json={"title": "x"}).json()
        response = client.patch(f"/notes/{created['id']}", json={"title": "y"})

        assert response.status_code == 422


class TestRpcStyle:
    def test_crud_round_trip(self) -> None:
        client = TestClient(_build_app("rpc"))

        created = client.post("/notes/create", json={"title": "hello"})
        assert created.status_code == 200
        note = created.json()

        fetched = client.post("/notes/get", json={"id": note["id"]})
        assert fetched.status_code == 200
        assert fetched.json()["title"] == "hello"

        updated = client.post(
            "/notes/update",
            json={"id": note["id"], "rev": note["rev"], "dto": {"title": "world"}},
        )
        assert updated.status_code == 200
        assert updated.json()["data"]["title"] == "world"

        listed = client.post("/notes/list", json={})
        assert listed.status_code == 200
        assert listed.json()["count"] == 1

        killed = client.post("/notes/kill", json={"id": note["id"]})
        assert killed.status_code == 204

    def test_soft_delete_and_restore(self) -> None:
        client = TestClient(_build_app("rpc"))

        note = client.post("/notes/create", json={"title": "x"}).json()

        deleted = client.post(
            "/notes/delete", json={"id": note["id"], "rev": note["rev"]}
        )
        assert deleted.status_code == 200
        assert deleted.json()["is_deleted"] is True

        restored = client.post(
            "/notes/restore",
            json={"id": note["id"], "rev": deleted.json()["rev"]},
        )
        assert restored.status_code == 200
        assert restored.json()["is_deleted"] is False

    def test_every_operation_is_a_post(self) -> None:
        paths = _build_app("rpc").openapi()["paths"]

        assert set(paths) == {f"/notes/{op.value}" for op in _ALL_OPS}
        assert all(set(methods) == {"post"} for methods in paths.values())


class TestCatalogProjection:
    @pytest.mark.parametrize("style", ["rest", "rpc"])
    def test_operation_ids_are_registry_keys_verbatim(self, style: str) -> None:
        assert _operation_ids(_build_app(style)) == _ALL_OP_IDS

    def test_read_only_spec_attaches_only_reads(self) -> None:
        assert _operation_ids(_build_app("rest", writable=False)) == _READ_OP_IDS

    def test_include_narrows_to_subset(self) -> None:
        app = _build_app("rest", include={DocumentKernelOp.GET, "list"})

        assert _operation_ids(app) == {"notes.get", "notes.list"}

    def test_include_of_unregistered_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="not registered"):
            _build_app("rest", writable=False, include={DocumentKernelOp.CREATE})

    def test_include_of_unknown_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="Unknown operations"):
            _build_app("rest", include={"nope"})

    def test_empty_namespace_raises(self) -> None:
        from forze.base.primitives import StrKeyNamespace

        spec = _spec()

        with pytest.raises(CoreException, match="No matching operations"):
            attach_document_routes(
                APIRouter(),
                registry=_registry(spec),
                ns=StrKeyNamespace(prefix="other"),
                ctx_dep=lambda: context_from_modules(MockDepsModule()),
                style="rest",
            )
