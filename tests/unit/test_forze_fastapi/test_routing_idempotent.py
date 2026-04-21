"""Unit tests for idempotency behavior on composed HTTP endpoints."""

from datetime import timedelta
from typing import Optional

from fastapi import APIRouter, FastAPI
from starlette.testclient import TestClient

from forze.application.contracts.idempotency import IdempotencyDepKey, IdempotencySnapshot
from forze.application.execution import Deps, ExecutionContext
from forze.application.composition.document import DocumentDTOs, build_document_registry
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import UsecasePlan
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.endpoints.document import attach_document_endpoints
from forze_fastapi.endpoints.http import IDEMPOTENCY_KEY_HEADER

# ----------------------- #


class _SpyIdempotencyPort:
    """Idempotency test double that records begin/commit calls."""

    def __init__(self) -> None:
        self.begin_calls: list[tuple[str, Optional[str], str]] = []
        self.commit_calls: list[tuple[str, Optional[str], str, IdempotencySnapshot]] = []

    async def begin(
        self,
        op: str,
        key: Optional[str],
        payload_hash: str,
    ) -> Optional[IdempotencySnapshot]:
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
    """Factory test double that always returns the same spy port."""

    def __init__(self, port: _SpyIdempotencyPort) -> None:
        self.port = port
        self.calls: list[timedelta] = []

    def __call__(
        self,
        context: ExecutionContext,
        ttl: timedelta = timedelta(seconds=30),
    ) -> _SpyIdempotencyPort:
        self.calls.append(ttl)
        return self.port


def _doc_spec_and_dtos() -> tuple[DocumentSpec, DocumentDTOs]:
    class UpdateCmd(BaseDTO):
        title: str | None = None

    spec = DocumentSpec(
        name="test",
        read=ReadDocument,
        write={
            "domain": Document,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": UpdateCmd,
        },
    )
    dtos = DocumentDTOs(
        read=ReadDocument,
        create=CreateDocumentCmd,
        update=UpdateCmd,
    )
    return spec, dtos


class TestDocumentCreateIdempotencyIntegration:
    """Invalid JSON must fail validation before idempotency runs."""

    def test_invalid_json_bypasses_idempotency_snapshotting(self) -> None:
        """Malformed body yields 422 and never calls the idempotency port."""
        spy_port = _SpyIdempotencyPort()
        spy_factory = _SpyIdempotencyFactory(spy_port)

        def _ctx_factory() -> ExecutionContext:
            return ExecutionContext(
                deps=Deps.plain(
                    {
                        IdempotencyDepKey: spy_factory,
                    }
                )
            )

        spec, dtos = _doc_spec_and_dtos()
        reg = build_document_registry(spec, dtos).extend_plan(
            UsecasePlan().tx("*", route="mock")
        )
        reg.finalize(spec.name, inplace=True)

        app = FastAPI()
        router = APIRouter(prefix="/api")
        attach_document_endpoints(
            router,
            document=spec,
            dtos=dtos,
            registry=reg,
            ctx_dep=_ctx_factory,
            endpoints={
                "get_": False,
                "list_": False,
                "raw_list": False,
                "create": True,
            },
        )
        app.include_router(router)
        client = TestClient(app)

        response_1 = client.post(
            "/api/create",
            data=b'{"name":',
            headers={
                IDEMPOTENCY_KEY_HEADER: "req-1",
                "Content-Type": "application/json",
            },
        )
        response_2 = client.post(
            "/api/create",
            data=b'{"name":',
            headers={
                IDEMPOTENCY_KEY_HEADER: "req-1",
                "Content-Type": "application/json",
            },
        )

        assert response_1.status_code == 422
        assert response_2.status_code == 422
        assert spy_port.begin_calls == []
        assert spy_port.commit_calls == []
