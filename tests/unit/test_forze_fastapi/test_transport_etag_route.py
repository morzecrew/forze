"""Unit tests for transport-layer ETag helpers and document GET integration."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import orjson
import pytest
from fastapi import APIRouter

from forze.application.composition.document import DocumentDTOs, DocumentFacade, build_document_registry
from forze.application.contracts.document import DocumentSpec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_fastapi.transport.http import attach_document_routes, make_facade_dep
from forze_fastapi.transport.http.etag.provider import document_etag
from forze_fastapi.transport.http.etag.utils import ensure_quoted_etag, etag_matches
from registry_helpers import freeze_registry

pytestmark = pytest.mark.unit


class TestTransportETagUtils:
    def test_ensure_quoted(self) -> None:
        assert ensure_quoted_etag("abc") == '"abc"'

    def test_etag_matches_star(self) -> None:
        assert etag_matches('"x"', "*")


class TestDocumentGetETagTransport:
    def test_document_etag_provider(self, composition_ctx) -> None:
        spec = DocumentSpec(
            name="test",
            read=ReadDocument,
            write={
                "domain": Document,
                "create_cmd": CreateDocumentCmd,
                "update_cmd": type("EmptyUpdate", (BaseDTO,), {}),
            },
        )
        dtos = DocumentDTOs(
            read=ReadDocument,
            create=CreateDocumentCmd,
            update=type("EmptyUpdate", (BaseDTO,), {}),
        )
        reg = freeze_registry(build_document_registry(spec, dtos))

        def ctx_dep():
            return composition_ctx

        facade_dep = make_facade_dep(
            DocumentFacade,
            registry=reg,
            namespace=spec.default_namespace,
            ctx_dep=ctx_dep,
        )
        router = APIRouter(prefix="/api")
        attach_document_routes(
            router,
            document=spec,
            dtos=dtos,
            facade_dep=facade_dep,
            ctx_dep=ctx_dep,
            registry=reg,
            enable=("get",),
            config={"enable_etag": True},
        )
        pk = uuid4()
        now = datetime.now(timezone.utc)
        doc = ReadDocument(id=pk, rev=1, created_at=now, last_update_at=now)
        tag = document_etag(orjson.dumps(doc.model_dump(mode="json")))
        assert tag is not None
        assert etag_matches(ensure_quoted_etag(tag), ensure_quoted_etag(tag))
