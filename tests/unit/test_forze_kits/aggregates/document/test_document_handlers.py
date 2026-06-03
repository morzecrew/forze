"""Unit tests for forze_kits.aggregates.document.handlers."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentQueryPort
from forze.base.exceptions import CoreException
from forze.domain.models import CreateDocumentCmd, ReadDocument
from forze_kits.aggregates.document import (
    CursorListRequestDTO,
    DocumentIdDTO,
    ListRequestDTO,
    ProjectedCursorListRequestDTO,
    ProjectedListRequestDTO,
)
from forze_kits.aggregates.document.handlers import (
    CursorListDocuments,
    GetDocument,
    ListDocuments,
    ProjectedCursorListDocuments,
    ProjectedListDocuments,
)

# ----------------------- #


class TestGetDocument:
    @pytest.mark.asyncio
    async def test_get_returns_document(
        self,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        doc_port = stub_document_port
        cmd = CreateDocumentCmd()
        created = await doc_port.create(cmd)
        pk = created.id

        handler = GetDocument(doc=doc_port)
        result = await handler(DocumentIdDTO(id=pk))

        assert result.id == pk
        assert result.rev == 1

    @pytest.mark.asyncio
    async def test_get_missing_raises(
        self,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        handler = GetDocument(doc=stub_document_port)
        with pytest.raises(CoreException, match="not found"):
            await handler(DocumentIdDTO(id=uuid4()))


class TestListDocuments:
    @pytest.mark.asyncio
    async def test_list_paginates(
        self,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        port = stub_document_port
        for _ in range(3):
            await port.create(CreateDocumentCmd())

        handler = ListDocuments[ReadDocument](doc=port)
        page1 = await handler(ListRequestDTO(page=1, size=2))
        assert len(page1.hits) == 2
        assert page1.count == 3
        assert page1.page == 1
        assert page1.size == 2

        page2 = await handler(ListRequestDTO(page=2, size=2))
        assert len(page2.hits) == 1
        assert page2.count == 3

    @pytest.mark.asyncio
    async def test_list_invokes_optional_mapper(
        self,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        mapper = AsyncMock(side_effect=lambda body: body)
        handler = ListDocuments[ReadDocument](
            doc=stub_document_port,
            mapper=mapper,
        )
        await handler(ListRequestDTO(page=1, size=10))
        mapper.assert_awaited_once()


class TestCursorListDocuments:
    @pytest.mark.asyncio
    async def test_cursor_list_returns_cursor_paginated(
        self,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        port = stub_document_port
        for _ in range(2):
            await port.create(CreateDocumentCmd())

        handler = CursorListDocuments[ReadDocument](doc=port)
        result = await handler(CursorListRequestDTO(limit=10))

        assert result.has_more in (True, False)
        assert len(result.hits) >= 1

    @pytest.mark.asyncio
    async def test_cursor_list_invokes_optional_mapper(
        self,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        mapper = AsyncMock(side_effect=lambda body: body)
        handler = CursorListDocuments[ReadDocument](
            doc=stub_document_port,
            mapper=mapper,
        )
        await handler(CursorListRequestDTO(limit=5))
        mapper.assert_awaited_once()


class TestProjectedCursorListDocuments:
    @pytest.mark.asyncio
    async def test_projected_cursor_list_returns_projection(
        self,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        port = stub_document_port
        await port.create(CreateDocumentCmd())

        handler = ProjectedCursorListDocuments(doc=port)
        result = await handler(
            ProjectedCursorListRequestDTO(
                return_fields={"id", "rev"},
                limit=5,
            )
        )
        assert "id" in result.hits[0]


class TestProjectedListDocuments:
    @pytest.mark.asyncio
    async def test_projected_list_returns_projection_page(
        self,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        port = stub_document_port
        await port.create(CreateDocumentCmd())

        handler = ProjectedListDocuments(doc=port)
        result = await handler(
            ProjectedListRequestDTO(page=1, size=10, return_fields={"id", "rev"}),
        )

        assert result.count >= 1
        assert result.hits
        assert set(result.hits[0].keys()) <= {"id", "rev"}

    @pytest.mark.asyncio
    async def test_projected_list_invokes_optional_mapper(
        self,
        stub_document_port: DocumentQueryPort,
    ) -> None:
        mapper = AsyncMock(side_effect=lambda body: body)
        handler = ProjectedListDocuments(
            doc=stub_document_port,
            mapper=mapper,
        )

        await handler(
            ProjectedListRequestDTO(page=1, size=1, return_fields={"id"}),
        )

        mapper.assert_awaited_once()
