"""Unit tests for transport-layer idempotency on document create."""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

import pytest
from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.composition.document import DocumentDTOs, DocumentFacade, build_document_registry
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.idempotency import IdempotencyDepKey, IdempotencySnapshot
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.transport.http import (
    IDEMPOTENCY_KEY_HEADER,
    attach_document_routes,
    make_facade_dep,
)
from registry_helpers import freeze_registry

pytestmark = pytest.mark.unit


class _SpyIdempotencyPort:
    def __init__(self) -> None:
        self.begin_calls: list[tuple[str, Optional[str], str]] = []
        self.commit_calls: list[tuple[str, Optional[str], str, IdempotencySnapshot]] = []

    async def begin(self, op: str, key: Optional[str], payload_hash: str) -> Optional[IdempotencySnapshot]:
        self.begin_calls.append((op, key, payload_hash))
        return None

    async def commit(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
        snapshot: IdempotencySnapshot,
    ) -> None:
        self.commit_calls.append((op, key, payload_hash, snapshot))


class _SpyIdempotencyFactory:
    def __init__(self, port: _SpyIdempotencyPort) -> None:
        self.port = port

    def __call__(self, context: ExecutionContext, ttl: timedelta = timedelta(seconds=30)) -> _SpyIdempotencyPort:
        _ = context, ttl
        return self.port


def _spec_and_dtos() -> tuple[DocumentSpec, DocumentDTOs]:
    class UpdateCmd(BaseDTO):
        title: str | None = None

    spec = DocumentSpec(
        name="test",
        read=ReadDocument,
        write={"domain": Document, "create_cmd": CreateDocumentCmd, "update_cmd": UpdateCmd},
    )
    dtos = DocumentDTOs(read=ReadDocument, create=CreateDocumentCmd, update=UpdateCmd)
    return spec, dtos


class TestDocumentCreateIdempotencyTransport:
    def test_invalid_json_skips_idempotency(self) -> None:
        spy_port = _SpyIdempotencyPort()
        spy_factory = _SpyIdempotencyFactory(spy_port)

        def _ctx_factory() -> ExecutionContext:
            return ExecutionContext(deps=Deps.plain({IdempotencyDepKey: spy_factory}))

        spec, dtos = _spec_and_dtos()
        reg = freeze_registry(build_document_registry(spec, dtos))
        facade_dep = make_facade_dep(
            DocumentFacade,
            registry=reg,
            namespace=spec.default_namespace,
            ctx_dep=_ctx_factory,
        )
        router = APIRouter(prefix="/api")
        attach_document_routes(
            router,
            document=spec,
            dtos=dtos,
            facade_dep=facade_dep,
            ctx_dep=_ctx_factory,
            registry=reg,
            enable=("create",),
            config={"enable_idempotency": True},
        )
        app = FastAPI()
        app.include_router(router)
        client = TestClient(app)

        for _ in range(2):
            response = client.post(
                "/api/create",
                data=b'{"name":',
                headers={IDEMPOTENCY_KEY_HEADER: "req-1", "Content-Type": "application/json"},
            )
            assert response.status_code == 422

        assert spy_port.begin_calls == []
        assert spy_port.commit_calls == []
