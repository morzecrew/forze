"""Unit tests for forze.application.usecases.document."""

from unittest.mock import AsyncMock

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentQueryPort
from forze.application.dto import (
    DocumentIdDTO,
    DocumentNumberIdDTO,
    ListRequestDTO,
    RawListRequestDTO,
)
from forze.application.usecases.document import (
    GetDocument,
    GetDocumentByNumberId,
    RawListDocuments,
    TypedListDocuments,
)
from forze.base.errors import NotFoundError
from forze.domain.models import CreateDocumentCmd, ReadDocument

# ----------------------- #


class TestGetDocument:
    """Tests for GetDocument usecase."""

    @pytest.mark.asyncio
    async def test_get_returns_document(
        self,
        stub_ctx,
        stub_document_port: DocumentQueryPort,
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
        stub_document_port: DocumentQueryPort,
    ) -> None:
        usecase = GetDocument(ctx=stub_ctx, doc=stub_document_port)
        with pytest.raises(NotFoundError, match="not found"):
            await usecase(DocumentIdDTO(id=uuid4()))


class TestTypedListDocuments:
    @pytest.mark.asyncio
    async def test_list_paginates(
        self,
        stub_ctx,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        port = stub_document_port
        for _ in range(3):
            await port.create(CreateDocumentCmd())

        uc = TypedListDocuments[ReadDocument](ctx=stub_ctx, doc=port)
        page1 = await uc(ListRequestDTO(page=1, size=2))
        assert len(page1.hits) == 2
        assert page1.count == 3
        assert page1.page == 1
        assert page1.size == 2

        page2 = await uc(ListRequestDTO(page=2, size=2))
        assert len(page2.hits) == 1
        assert page2.count == 3

    @pytest.mark.asyncio
    async def test_list_invokes_optional_mapper(
        self,
        stub_ctx,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        mapper = AsyncMock(side_effect=lambda body, ctx=None: body)
        uc = TypedListDocuments[ReadDocument](
            ctx=stub_ctx,
            doc=stub_document_port,
            mapper=mapper,
        )
        await uc(ListRequestDTO(page=1, size=10))
        mapper.assert_awaited_once()


class TestGetDocumentByNumberId:
    @pytest.mark.asyncio
    async def test_get_by_number_id_returns_document(self, stub_ctx) -> None:
        expected = object()
        doc_port = AsyncMock(spec=DocumentQueryPort)
        doc_port.find = AsyncMock(return_value=expected)

        usecase = GetDocumentByNumberId(ctx=stub_ctx, doc=doc_port)
        result = await usecase(DocumentNumberIdDTO(number_id=42))

        doc_port.find.assert_awaited_once_with(filters={"$fields": {"number_id": 42}})
        assert result is expected

    @pytest.mark.asyncio
    async def test_get_by_number_id_missing_raises(
        self,
        stub_ctx,
    ) -> None:
        doc_port = AsyncMock(spec=DocumentQueryPort)
        doc_port.find = AsyncMock(return_value=None)
        usecase = GetDocumentByNumberId(ctx=stub_ctx, doc=doc_port)

        with pytest.raises(NotFoundError, match="Document not found with number ID"):
            await usecase(DocumentNumberIdDTO(number_id=10_001))


class TestRawListDocuments:
    @pytest.mark.asyncio
    async def test_raw_list_returns_projection_page(
        self,
        stub_ctx,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        port = stub_document_port
        await port.create(CreateDocumentCmd())

        uc = RawListDocuments(ctx=stub_ctx, doc=port)
        result = await uc(
            RawListRequestDTO(page=1, size=10, return_fields={"id", "rev"}),
        )

        assert result.count >= 1
        assert result.hits
        assert set(result.hits[0].keys()) <= {"id", "rev"}

    @pytest.mark.asyncio
    async def test_raw_list_invokes_optional_mapper(
        self,
        stub_ctx,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        mapper = AsyncMock(side_effect=lambda body, ctx=None: body)
        uc = RawListDocuments(ctx=stub_ctx, doc=stub_document_port, mapper=mapper)

        await uc(RawListRequestDTO(page=1, size=1, return_fields={"id"}))

        mapper.assert_awaited_once()
