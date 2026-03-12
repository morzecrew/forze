"""Unit tests for forze.application.usecases.search."""

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import (
    SearchFieldSpec,
    SearchIndexSpec,
    SearchSpec,
)
from forze.application.dto import Paginated, RawPaginated, RawSearchRequestDTO, SearchRequestDTO
from forze.application.usecases.search import RawSearch, TypedSearch
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _SearchDoc(Document):
    """Document model with title and content for search tests."""

    title: str = ""
    content: str = ""


class _SearchCreate(CreateDocumentCmd):
    """Create command for search test documents."""

    title: str = ""
    content: str = ""


class _SearchUpdate(BaseDTO):
    """Update command for search test documents."""

    title: str | None = None
    content: str | None = None


class _SearchRead(ReadDocument):
    """Read model for search test documents."""

    title: str = ""
    content: str = ""


class _HitModel(BaseModel):
    """Search hit model for TypedSearch tests."""

    id: UUID
    title: str


def _search_document_spec() -> DocumentSpec:
    """DocumentSpec for search tests (namespace shared with search)."""
    return DocumentSpec(
        namespace="search_test",
        read={"source": "search_read", "model": _SearchRead},
        write={
            "source": "search_write",
            "models": {
                "domain": _SearchDoc,
                "create_cmd": _SearchCreate,
                "update_cmd": _SearchUpdate,
            },
        },
    )


def _search_spec() -> SearchSpec[_HitModel]:
    """SearchSpec for search tests."""
    return SearchSpec(
        namespace="search_test",
        model=_HitModel,
        indexes={
            "main": SearchIndexSpec(
                fields=[
                    SearchFieldSpec(path="title"),
                    SearchFieldSpec(path="content"),
                ]
            ),
        },
        default_index="main",
    )


class TestTypedSearch:
    """Tests for TypedSearch usecase."""

    @pytest.mark.asyncio
    async def test_typed_search_returns_paginated(
        self,
        stub_ctx,
    ) -> None:
        doc_port = stub_ctx.doc_write(_search_document_spec())
        search_port = stub_ctx.search(_search_spec())

        # Seed documents with content matching "foo"
        await doc_port.create(_SearchCreate(title="a", content="foo"))
        await doc_port.create(_SearchCreate(title="b", content="foo"))

        usecase = TypedSearch(ctx=stub_ctx, search=search_port)
        args = SearchRequestDTO(query="foo", page=1, size=10)
        result = await usecase(args)

        assert isinstance(result, Paginated)
        assert result.page == 1
        assert result.size == 10
        assert result.count == 2
        assert len(result.hits) == 2
        titles = sorted(h.title for h in result.hits)
        assert titles == ["a", "b"]

    @pytest.mark.asyncio
    async def test_typed_search_empty_query_returns_default(
        self,
        stub_ctx,
    ) -> None:
        doc_port = stub_ctx.doc_write(_search_document_spec())
        search_port = stub_ctx.search(_search_spec())

        # Empty query matches all docs; create one
        await doc_port.create(_SearchCreate(title="x", content=""))

        usecase = TypedSearch(ctx=stub_ctx, search=search_port)
        args = SearchRequestDTO(query="", page=1, size=10)
        result = await usecase(args)

        assert result.count == 1
        assert result.hits[0].title == "x"


class TestRawSearch:
    """Tests for RawSearch usecase."""

    @pytest.mark.asyncio
    async def test_raw_search_returns_raw_paginated(
        self,
        stub_ctx,
    ) -> None:
        doc_port = stub_ctx.doc_write(_search_document_spec())
        search_port = stub_ctx.search(_search_spec())

        await doc_port.create(_SearchCreate(title="x", content="bar"))
        await doc_port.create(_SearchCreate(title="y", content="bar"))

        usecase = RawSearch(ctx=stub_ctx, search=search_port)
        args = RawSearchRequestDTO(query="bar", return_fields={"id", "title"}, page=1, size=10)
        result = await usecase(args)

        assert isinstance(result, RawPaginated)
        assert result.page == 1
        assert result.size == 10
        assert result.count == 2
        assert len(result.hits) == 2
        assert "id" in result.hits[0]
        assert result.hits[0]["title"] == "x"

    @pytest.mark.asyncio
    async def test_raw_search_respects_pagination(
        self,
        stub_ctx,
    ) -> None:
        doc_port = stub_ctx.doc_write(_search_document_spec())
        search_port = stub_ctx.search(_search_spec())

        for i in range(5):
            await doc_port.create(_SearchCreate(title="", content="q"))

        usecase = RawSearch(ctx=stub_ctx, search=search_port)
        args = RawSearchRequestDTO(query="q", return_fields={"content"}, page=2, size=2)
        result = await usecase(args)

        assert result.count == 5
        assert len(result.hits) == 2
