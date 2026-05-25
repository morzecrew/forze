"""Unit tests for ``forze_fastapi.transport.http`` core (PR1)."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import attrs
import pytest
from fastapi import APIRouter, Depends, FastAPI
from registry_helpers import freeze_registry
from starlette.testclient import TestClient

from forze.application.composition.document import DocumentFacade
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.execution import Handler
from forze.application.execution.registry import OperationRegistry
from forze.application.execution.running import run_operation
from forze.application.handlers.document import DocumentIdDTO
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.transport.http import make_facade_dep

# ----------------------- #

pytestmark = pytest.mark.unit


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _StubGetHandler(Handler[DocumentIdDTO, ReadDocument]):
    async def __call__(self, args: DocumentIdDTO) -> ReadDocument:
        now = datetime.now(timezone.utc)
        return ReadDocument(id=args.id, rev=1, created_at=now, last_update_at=now)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _EchoHandler(Handler[BaseDTO, str]):
    message: str

    async def __call__(self, args: BaseDTO) -> str:
        return self.message


def _minimal_spec() -> DocumentSpec:
    return DocumentSpec(
        name="test",
        read=ReadDocument,
        write={
            "domain": Document,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": type("EmptyUpdate", (BaseDTO,), {}),
        },
    )


# ....................... #


class TestRunOperation:
    async def test_run_operation_invokes_registry(self, composition_ctx) -> None:
        reg = OperationRegistry(
            handlers={"echo": lambda _ctx: _EchoHandler(message="ok")},
        )
        frozen = freeze_registry(reg)

        result = await run_operation(frozen, "echo", BaseDTO(), ctx=composition_ctx)

        assert result == "ok"


class TestMakeFacadeDep:
    def test_make_facade_dep_yields_facade(self, composition_ctx) -> None:
        spec = _minimal_spec()
        reg = OperationRegistry(
            handlers={
                spec.default_namespace.key("get"): lambda _ctx: _StubGetHandler(),
            },
        )
        frozen = freeze_registry(reg)

        def ctx_dep():
            return composition_ctx

        dep = make_facade_dep(
            DocumentFacade,
            registry=frozen,
            namespace=spec.default_namespace,
            ctx_dep=ctx_dep,
        )
        facade = dep(ctx=composition_ctx)
        assert isinstance(facade, DocumentFacade)
        assert facade.ctx is composition_ctx


class TestHandwrittenDocumentGetRoute:
    def test_handwritten_document_get_route(
        self,
        composition_ctx,
    ) -> None:
        spec = _minimal_spec()
        reg = OperationRegistry(
            handlers={
                spec.default_namespace.key("get"): lambda _ctx: _StubGetHandler(),
            },
        )
        frozen = freeze_registry(reg)

        def ctx_dep():
            return composition_ctx

        doc_dep = make_facade_dep(
            DocumentFacade,
            registry=frozen,
            namespace=spec.default_namespace,
            ctx_dep=ctx_dep,
        )

        router = APIRouter(prefix="/docs")

        @router.get("/get")
        async def get_doc(
            query: DocumentIdDTO = Depends(),
            doc: DocumentFacade = Depends(doc_dep),
        ) -> ReadDocument:
            return await doc.get(query)

        app = FastAPI()
        app.include_router(router)

        doc_id = uuid4()
        with TestClient(app) as client:
            response = client.get(f"/docs/get?id={doc_id}")

        assert response.status_code == 200
        body = response.json()
        assert body["id"] == str(doc_id)
