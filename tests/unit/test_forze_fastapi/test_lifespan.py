"""Tests for the runtime lifespan helper (forze_fastapi.runtime_lifespan)."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")

from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import ExecutionRuntime, build_runtime
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_fastapi import runtime_lifespan
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_document_routes
from forze_kits.aggregates.document import DocumentDTOs, build_document_registry
from forze_mock import MockDepsModule

# ----------------------- #


class _Note(Document):
    title: str = ""


class _NoteCreate(BaseDTO):
    title: str = ""


class _NoteRead(ReadDocument):
    title: str


# ....................... #


def _counting_runtime() -> tuple[ExecutionRuntime, dict[str, int]]:
    counts = {"started": 0, "stopped": 0}

    async def up(_ctx) -> None:
        counts["started"] += 1

    async def down(_ctx) -> None:
        counts["stopped"] += 1

    runtime = build_runtime(
        MockDepsModule(),
        lifecycle_steps=[LifecycleStep(id="probe", startup=up, shutdown=down)],
    )
    return runtime, counts


# ----------------------- #


class TestRuntimeLifespan:
    @pytest.mark.asyncio
    async def test_enters_and_exits_scope(self) -> None:
        runtime, counts = _counting_runtime()
        lifespan = runtime_lifespan(runtime)
        app = FastAPI()

        async with lifespan(app):
            assert counts == {"started": 1, "stopped": 0}
            assert runtime.get_context() is not None

        assert counts == {"started": 1, "stopped": 1}
        with pytest.raises(CoreException, match="not set"):
            runtime.get_context()

    @pytest.mark.asyncio
    async def test_exception_during_app_lifetime_still_shuts_down(self) -> None:
        runtime, counts = _counting_runtime()
        lifespan = runtime_lifespan(runtime)

        with pytest.raises(RuntimeError, match="app blew up"):
            async with lifespan(FastAPI()):
                raise RuntimeError("app blew up")

        assert counts == {"started": 1, "stopped": 1}
        with pytest.raises(CoreException, match="not set"):
            runtime.get_context()


class TestRuntimeLifespanWithFastAPI:
    def test_testclient_fires_startup_and_shutdown(self) -> None:
        runtime, counts = _counting_runtime()
        app = FastAPI(lifespan=runtime_lifespan(runtime))

        with TestClient(app):
            assert counts == {"started": 1, "stopped": 0}

        assert counts == {"started": 1, "stopped": 1}

    def test_generated_route_works_inside_lifespan(self) -> None:
        spec = DocumentSpec(
            name="notes",
            read=_NoteRead,
            write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteCreate),
        )
        registry = build_document_registry(
            spec, DocumentDTOs(read=_NoteRead, create=_NoteCreate)
        ).freeze()

        runtime = build_runtime(MockDepsModule())

        router = APIRouter(prefix="/notes")
        attach_document_routes(
            router,
            registry=registry,
            ns=spec.default_namespace,
            ctx_dep=runtime.get_context,
            style="rest",
        )

        app = FastAPI(lifespan=runtime_lifespan(runtime))
        app.include_router(router)
        register_exception_handlers(app)

        with TestClient(app) as client:
            created = client.post("/notes", json={"title": "hello"})
            assert created.status_code == 201
            note = created.json()

            fetched = client.get(f"/notes/{note['id']}")
            assert fetched.status_code == 200
            assert fetched.json()["title"] == "hello"
