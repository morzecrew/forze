"""Tests for generated search routes (forze_fastapi.routes.attach_search_routes)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

import asyncio

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_search_routes
from forze_kits.aggregates.search import SearchKernelOp, build_search_registry
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _NoteRead(ReadDocument):
    title: str


class _NoteCreate(BaseDTO):
    title: str = ""


class _Note(Document):
    title: str = ""


# ....................... #


def _search_spec() -> SearchSpec[_NoteRead]:
    # Same name as the document spec, so the mock search adapter scans the
    # documents seeded into the shared MockState.
    return SearchSpec(name="notes", model_type=_NoteRead, fields=["title"])


def _seed_note(state: MockState, title: str) -> None:
    doc_spec = DocumentSpec(
        name="notes",
        read=_NoteRead,
        write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
    )

    async def _create() -> None:
        ctx = context_from_modules(MockDepsModule(state=state))
        await ctx.document.command(doc_spec).create(_NoteCreate(title=title))

    asyncio.run(_create())


def _build_app(*, state: MockState | None = None, include=None) -> FastAPI:
    spec = _search_spec()
    state = state or MockState()

    router = APIRouter(prefix="/notes/search")
    attach_search_routes(
        router,
        registry=build_search_registry(spec).freeze(),
        ns=spec.default_namespace,
        ctx_dep=lambda: context_from_modules(MockDepsModule(state=state)),
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


# ....................... #


class TestSearchRoutes:
    def test_typed_search_round_trip(self) -> None:
        state = MockState()
        _seed_note(state, "hello world")
        _seed_note(state, "unrelated")

        client = TestClient(_build_app(state=state))
        response = client.post("/notes/search/typed", json={"query": "hello"})

        assert response.status_code == 200
        assert [hit["title"] for hit in response.json()["hits"]] == ["hello world"]

    def test_raw_search_projects_fields(self) -> None:
        state = MockState()
        _seed_note(state, "hello world")

        client = TestClient(_build_app(state=state))
        response = client.post(
            "/notes/search/raw",
            json={"query": "hello", "return_fields": ["title"]},
        )

        assert response.status_code == 200
        assert response.json()["hits"] == [{"title": "hello world"}]

    def test_every_operation_is_a_post(self) -> None:
        paths = _build_app().openapi()["paths"]

        assert set(paths) == {f"/notes/search/{op.value}" for op in SearchKernelOp}
        assert all(set(methods) == {"post"} for methods in paths.values())

    def test_operation_ids_are_registry_keys_verbatim(self) -> None:
        expected = {f"notes.{op.value}" for op in SearchKernelOp}

        assert _operation_ids(_build_app()) == expected

    def test_include_narrows_to_subset(self) -> None:
        app = _build_app(include={SearchKernelOp.TYPED})

        assert _operation_ids(app) == {"notes.typed"}

    def test_include_of_unknown_operation_raises(self) -> None:
        with pytest.raises(CoreException, match="Unknown operations"):
            _build_app(include={"nope"})
