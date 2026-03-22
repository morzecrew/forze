"""Unit tests for forze.application.usecases.document."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentReadPort
from forze.application.dto import DocumentIdDTO
from forze.application.usecases.document import GetDocument
from forze.base.errors import NotFoundError
from forze.domain.models import CreateDocumentCmd

# ----------------------- #


class TestGetDocument:
    """Tests for GetDocument usecase."""

    @pytest.mark.asyncio
    async def test_get_returns_document(
        self,
        stub_ctx,
        stub_document_port: DocumentReadPort,
    ) -> None:
        doc_port = stub_document_port
        cmd = CreateDocumentCmd()
        created = await doc_port.create(cmd)
        pk = created.id

        usecase = GetDocument(ctx=stub_ctx, doc=doc_port)
        result = await usecase(DocumentIdDTO(id=pk))

        assert result.id == pk
        assert result.rev == 1

    @pytest.mark.asyncio
    async def test_get_missing_raises(
        self,
        stub_ctx,
        stub_document_port: DocumentReadPort,
    ) -> None:
        usecase = GetDocument(ctx=stub_ctx, doc=stub_document_port)
        with pytest.raises(NotFoundError, match="not found"):
            await usecase(DocumentIdDTO(id=uuid4()))
