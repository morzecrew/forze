"""Tests for the aggregate-kit routes emitter (forze_fastapi.routes.attach_aggregate_routes)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec
from forze.domain.models import CreateDocumentCmd, ReadDocument
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_aggregate_routes
from forze_kits.aggregates import AggregateKit
from forze_kits.domain.soft_deletion.models import (
    DocWithSoftDeletion,
    UpdateCmdWithSoftDeletion,
)
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #

_TX = "mock"


class _Note(DocWithSoftDeletion):
    title: str = ""


class _NoteCreate(CreateDocumentCmd):
    title: str


class _NoteUpdate(UpdateCmdWithSoftDeletion):
    title: str | None = None


class _NoteRead(ReadDocument):
    title: str = ""
    is_deleted: bool = False


_NOTE_SPEC = DocumentSpec(
    name="notes",
    read=_NoteRead,
    write=DocumentWriteTypes(
        domain=_Note, create_cmd=_NoteCreate, update_cmd=_NoteUpdate
    ),
)
_NOTE_INDEX = SearchSpec(name="notes_index", model_type=_NoteRead, fields=["title"])


def _kit(*, search: bool = True) -> AggregateKit[_NoteRead, _Note, _NoteCreate, _NoteUpdate]:
    return AggregateKit(
        spec=_NOTE_SPEC,
        soft_delete=True,
        search=_NOTE_INDEX if search else None,
    )


def _app(*, search: bool = True) -> tuple[FastAPI, MockState]:
    state = MockState()
    router = APIRouter(prefix="/notes")
    attach_aggregate_routes(
        router,
        _kit(search=search),
        ctx_dep=lambda: context_from_modules(MockDepsModule(state=state)),
        style="rest",
        tx_route=_TX,
    )
    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    return app, state


def _operation_ids(app: FastAPI) -> set[str]:
    return {
        operation["operationId"]
        for methods in app.openapi()["paths"].values()
        for operation in methods.values()
    }


# ....................... #


class TestAttachAggregateRoutes:
    def test_projects_document_softdelete_and_search_ops(self) -> None:
        ids = _operation_ids(_app()[0])

        # document CRUD
        assert {"notes.create", "notes.get", "notes.update", "notes.list"} <= ids
        # soft-delete (merged in by the kit)
        assert {"notes.delete", "notes.restore"} <= ids
        # external search query ops (under the search spec's namespace)
        assert "notes_index.typed" in ids

    def test_no_search_omits_search_routes(self) -> None:
        ids = _operation_ids(_app(search=False)[0])

        assert "notes.create" in ids
        assert not any(op.startswith("notes_index.") for op in ids)

    def test_storage_routes_are_projected_when_declared(self) -> None:
        state = MockState()
        router = APIRouter(prefix="/notes")
        kit = AggregateKit(
            spec=_NOTE_SPEC, soft_delete=True, storage=StorageSpec(name="notes_blobs")
        )
        attach_aggregate_routes(
            router,
            kit,
            ctx_dep=lambda: context_from_modules(MockDepsModule(state=state)),
            style="rest",
            tx_route=_TX,
        )
        app = FastAPI()
        app.include_router(router)

        ids = _operation_ids(app)
        assert "notes.create" in ids  # document surface
        assert "notes_blobs.upload" in ids and "notes_blobs.download" in ids  # blob surface

    @pytest.mark.parametrize("prefix", ["", "/", "blobs"])
    def test_root_like_storage_prefix_is_rejected(self, prefix: str) -> None:
        from forze.base.exceptions import CoreException, ExceptionKind

        kit = AggregateKit(spec=_NOTE_SPEC, storage=StorageSpec(name="notes_blobs"))
        with pytest.raises(CoreException) as ei:
            attach_aggregate_routes(
                APIRouter(prefix="/notes"),
                kit,
                ctx_dep=lambda: context_from_modules(MockDepsModule()),
                style="rest",
                tx_route=_TX,
                storage_prefix=prefix,
            )
        assert ei.value.kind is ExceptionKind.CONFIGURATION

    def test_create_then_get_round_trips_through_the_routes(self) -> None:
        client = TestClient(_app()[0])

        created = client.post("/notes", json={"title": "hello"})
        assert created.status_code == 201  # REST create → 201 Created
        note_id = created.json()["id"]

        fetched = client.get(f"/notes/{note_id}")
        assert fetched.status_code == 200
        assert fetched.json()["title"] == "hello"
