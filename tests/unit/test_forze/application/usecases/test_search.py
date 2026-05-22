"""Unit tests for forze.application.handlers.search."""

from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
from forze.application.dto.paginated import CursorPaginated, Paginated
from forze.application.handlers.search import (
    CursorSearch,
    ProjectedCursorSearch,
    ProjectedSearch,
    Search,
)
from forze.application.handlers.search.dto import (
    CursorSearchRequestDTO,
    ProjectedCursorSearchRequestDTO,
    ProjectedSearchRequestDTO,
    ProjectedSearchPaginated,
    SearchPaginated,
    SearchRequestDTO,
)
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument

# ----------------------- #


class _SearchDoc(Document):
    title: str = ""
    content: str = ""


class _SearchCreate(CreateDocumentCmd):
    title: str = ""
    content: str = ""


class _SearchUpdate(BaseDTO):
    title: str | None = None
    content: str | None = None


class _SearchRead(ReadDocument):
    title: str = ""
    content: str = ""


class _HitModel(BaseModel):
    id: UUID
    title: str


def _search_document_spec() -> DocumentSpec:
    return DocumentSpec(
        name="search_test",
        read=_SearchRead,
        write={
            "domain": _SearchDoc,
            "create_cmd": _SearchCreate,
            "update_cmd": _SearchUpdate,
        },
    )


def _search_spec() -> SearchSpec[_HitModel]:
    return SearchSpec(
        name="search_test",
        model_type=_HitModel,
        fields=["title", "content"],
    )


class TestSearch:
    @pytest.mark.asyncio
    async def test_search_returns_paginated(self, stub_ctx) -> None:
        doc_port = stub_ctx.document.command(_search_document_spec())
        search_port = stub_ctx.search.query(_search_spec())

        await doc_port.create(_SearchCreate(title="a", content="foo"))
        await doc_port.create(_SearchCreate(title="b", content="foo"))

        handler = Search[_HitModel](search=search_port)
        result = await handler(SearchRequestDTO(query="foo", page=1, size=10))

        assert isinstance(result, SearchPaginated)
        assert result.page == 1
        assert result.size == 10
        assert result.count == 2
        assert len(result.hits) == 2
        assert sorted(h.title for h in result.hits) == ["a", "b"]

    @pytest.mark.asyncio
    async def test_search_disjunctive_list_query(self, stub_ctx) -> None:
        doc_port = stub_ctx.document.command(_search_document_spec())
        search_port = stub_ctx.search.query(_search_spec())

        await doc_port.create(_SearchCreate(title="only_a", content="alpha"))
        await doc_port.create(_SearchCreate(title="only_b", content="beta"))

        handler = Search[_HitModel](search=search_port)
        result = await handler(
            SearchRequestDTO(query=["alpha", "beta"], page=1, size=10)
        )

        assert result.count == 2
        assert {h.title for h in result.hits} == {"only_a", "only_b"}

    @pytest.mark.asyncio
    async def test_search_empty_query_returns_default(self, stub_ctx) -> None:
        doc_port = stub_ctx.document.command(_search_document_spec())
        search_port = stub_ctx.search.query(_search_spec())

        await doc_port.create(_SearchCreate(title="x", content=""))

        handler = Search[_HitModel](search=search_port)
        result = await handler(SearchRequestDTO(query="", page=1, size=10))

        assert result.count == 1
        assert result.hits[0].title == "x"


class TestProjectedSearch:
    @pytest.mark.asyncio
    async def test_projected_search_returns_raw_paginated(self, stub_ctx) -> None:
        doc_port = stub_ctx.document.command(_search_document_spec())
        search_port = stub_ctx.search.query(_search_spec())

        await doc_port.create(_SearchCreate(title="x", content="bar"))
        await doc_port.create(_SearchCreate(title="y", content="bar"))

        handler = ProjectedSearch(search=search_port)
        result = await handler(
            ProjectedSearchRequestDTO(
                query="bar", return_fields={"id", "title"}, page=1, size=10
            )
        )

        assert isinstance(result, ProjectedSearchPaginated)
        assert result.page == 1
        assert result.count == 2
        assert "id" in result.hits[0]
        assert result.hits[0]["title"] == "x"

    @pytest.mark.asyncio
    async def test_projected_search_respects_pagination(self, stub_ctx) -> None:
        doc_port = stub_ctx.document.command(_search_document_spec())
        search_port = stub_ctx.search.query(_search_spec())

        for _ in range(5):
            await doc_port.create(_SearchCreate(title="", content="q"))

        handler = ProjectedSearch(search=search_port)
        result = await handler(
            ProjectedSearchRequestDTO(
                query="q", return_fields={"content"}, page=2, size=2
            )
        )

        assert result.count == 5
        assert len(result.hits) == 2


class TestCursorSearch:
    @pytest.mark.asyncio
    async def test_cursor_search_returns_cursor_paginated(self, stub_ctx) -> None:
        doc_port = stub_ctx.document.command(_search_document_spec())
        search_port = stub_ctx.search.query(_search_spec())

        await doc_port.create(_SearchCreate(title="a", content="foo"))

        handler = CursorSearch[_HitModel](search=search_port)
        result = await handler(CursorSearchRequestDTO(query="foo", limit=10))

        assert isinstance(result, CursorPaginated)
        assert len(result.hits) >= 1
        assert result.hits[0].title == "a"


class TestProjectedCursorSearch:
    @pytest.mark.asyncio
    async def test_projected_cursor_search_returns_projection(self, stub_ctx) -> None:
        doc_port = stub_ctx.document.command(_search_document_spec())
        search_port = stub_ctx.search.query(_search_spec())

        await doc_port.create(_SearchCreate(title="x", content="bar"))

        handler = ProjectedCursorSearch(search=search_port)
        result = await handler(
            ProjectedCursorSearchRequestDTO(
                query="bar", return_fields={"id", "title"}, limit=10
            )
        )

        assert "id" in result.hits[0]
        assert result.hits[0]["title"] == "x"
