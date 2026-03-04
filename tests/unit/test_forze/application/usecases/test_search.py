"""Unit tests for forze.application.usecases.search."""

import pytest
from pydantic import BaseModel

from forze.application.dto import Paginated, RawPaginated, SearchRequestDTO
from forze.application.usecases.search import RawSearch, TypedSearch

# ----------------------- #


class TestTypedSearch:
    """Tests for TypedSearch usecase."""

    @pytest.mark.asyncio
    async def test_typed_search_returns_paginated(
        self,
        stub_ctx,
        stub_search_port,
    ) -> None:
        class HitModel(BaseModel):
            id: str
            title: str

        stub_search_port.add_hits("foo", [
            HitModel(id="1", title="a"),
            HitModel(id="2", title="b"),
        ])

        usecase = TypedSearch(ctx=stub_ctx, search=stub_search_port)
        args: dict = {
            "body": SearchRequestDTO(query="foo"),
            "page": 1,
            "size": 10,
        }
        result = await usecase(args)

        assert isinstance(result, Paginated)
        assert result.page == 1
        assert result.size == 10
        assert result.count == 2
        assert len(result.hits) == 2
        assert result.hits[0].id == "1"
        assert result.hits[0].title == "a"

    @pytest.mark.asyncio
    async def test_typed_search_empty_query_returns_default(
        self,
        stub_ctx,
        stub_search_port,
    ) -> None:
        class HitModel(BaseModel):
            id: str

        stub_search_port.set_default_hits([HitModel(id="x")])

        usecase = TypedSearch(ctx=stub_ctx, search=stub_search_port)
        args: dict = {
            "body": SearchRequestDTO(query="unknown"),
            "page": 1,
            "size": 10,
        }
        result = await usecase(args)

        assert result.count == 1
        assert result.hits[0].id == "x"


class TestRawSearch:
    """Tests for RawSearch usecase."""

    @pytest.mark.asyncio
    async def test_raw_search_returns_raw_paginated(
        self,
        stub_ctx,
        stub_search_port,
    ) -> None:
        stub_search_port.add_hits("bar", [
            {"id": "1", "title": "x"},
            {"id": "2", "title": "y"},
        ])

        from forze.application.dto import RawSearchRequestDTO

        usecase = RawSearch(ctx=stub_ctx, search=stub_search_port)
        args: dict = {
            "body": RawSearchRequestDTO(query="bar", return_fields={"id", "title"}),
            "page": 1,
            "size": 10,
        }
        result = await usecase(args)

        assert isinstance(result, RawPaginated)
        assert result.page == 1
        assert result.size == 10
        assert result.count == 2
        assert len(result.hits) == 2
        assert result.hits[0] == {"id": "1", "title": "x"}

    @pytest.mark.asyncio
    async def test_raw_search_respects_pagination(
        self,
        stub_ctx,
        stub_search_port,
    ) -> None:
        stub_search_port.add_hits("q", [
            {"a": 1},
            {"a": 2},
            {"a": 3},
            {"a": 4},
            {"a": 5},
        ])

        from forze.application.dto import RawSearchRequestDTO

        usecase = RawSearch(ctx=stub_ctx, search=stub_search_port)
        args: dict = {
            "body": RawSearchRequestDTO(query="q", return_fields={"a"}),
            "page": 2,
            "size": 2,
        }
        result = await usecase(args)

        assert result.count == 5
        assert len(result.hits) == 2
        assert result.hits[0]["a"] == 3
        assert result.hits[1]["a"] == 4
