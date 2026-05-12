"""Tests for forze.application.contracts.search.ports (SearchQueryPort)."""

from __future__ import annotations

from collections.abc import Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.base import CursorPage, page_from_limit_offset
from forze.application.contracts.search import SearchQueryPort
from forze.application.contracts.search.types import SearchOptions


class _Hit(BaseModel):
    id: str


class _Alt(BaseModel):
    name: str


class _StubSearch:
    async def _offset(
        self,
        query: str | Sequence[str],
        filters,
        pagination,
        *,
        options: SearchOptions | None,
        return_count: bool,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ):
        _ = query, filters, pagination, options
        if return_fields is not None:
            data: list[object] = [{"id": "1"}]
        elif return_type is not None:
            data = [return_type(name="x")]
        else:
            data = [_Hit(id="1")]
        if return_count:
            return page_from_limit_offset(data, pagination, total=1)
        return page_from_limit_offset(data, pagination, total=None)

    async def search(
        self,
        query: str | Sequence[str],
        filters=None,
        pagination=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
        snapshot=None,
    ):
        _ = sorts, snapshot
        return await self._offset(
            query,
            filters,
            pagination,
            options=options,
            return_count=False,
            return_type=None,
            return_fields=None,
        )

    async def search_page(
        self,
        query: str | Sequence[str],
        filters=None,
        pagination=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
        snapshot=None,
    ):
        _ = sorts, snapshot
        return await self._offset(
            query,
            filters,
            pagination,
            options=options,
            return_count=True,
            return_type=None,
            return_fields=None,
        )

    async def project_search(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters=None,
        pagination=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
        snapshot=None,
    ):
        _ = sorts, snapshot, fields
        return await self._offset(
            query,
            filters,
            pagination,
            options=options,
            return_count=False,
            return_type=None,
            return_fields=("id",),
        )

    async def project_search_page(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters=None,
        pagination=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
        snapshot=None,
    ):
        _ = sorts, snapshot, fields
        return await self._offset(
            query,
            filters,
            pagination,
            options=options,
            return_count=True,
            return_type=None,
            return_fields=("id",),
        )

    async def select_search(
        self,
        return_type: type[BaseModel],
        query: str | Sequence[str],
        filters=None,
        pagination=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
        snapshot=None,
    ):
        _ = sorts, snapshot
        return await self._offset(
            query,
            filters,
            pagination,
            options=options,
            return_count=False,
            return_type=return_type,
            return_fields=None,
        )

    async def select_search_page(
        self,
        return_type: type[BaseModel],
        query: str | Sequence[str],
        filters=None,
        pagination=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
        snapshot=None,
    ):
        _ = sorts, snapshot
        return await self._offset(
            query,
            filters,
            pagination,
            options=options,
            return_count=True,
            return_type=return_type,
            return_fields=None,
        )

    async def _cursor(
        self,
        query: str | Sequence[str],
        filters,
        cursor,
        sorts,
        *,
        options: SearchOptions | None,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
    ):
        _ = query, filters, cursor, sorts, options
        if return_fields is not None:
            return CursorPage(
                hits=[{"id": "1"}],
                next_cursor=None,
                prev_cursor=None,
                has_more=False,
            )
        if return_type is not None:
            return CursorPage(
                hits=[return_type(name="x")],
                next_cursor=None,
                prev_cursor=None,
                has_more=False,
            )  # type: ignore[valid-type]
        return CursorPage(
            hits=[_Hit(id="1")],
            next_cursor=None,
            prev_cursor=None,
            has_more=False,
        )

    async def search_cursor(
        self,
        query: str | Sequence[str],
        filters=None,
        cursor=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
    ):
        return await self._cursor(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=None,
            return_fields=None,
        )

    async def project_search_cursor(
        self,
        fields: Sequence[str],
        query: str | Sequence[str],
        filters=None,
        cursor=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
    ):
        _ = fields
        return await self._cursor(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=None,
            return_fields=("id",),
        )

    async def select_search_cursor(
        self,
        return_type: type[BaseModel],
        query: str | Sequence[str],
        filters=None,
        cursor=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
    ):
        return await self._cursor(
            query,
            filters,
            cursor,
            sorts,
            options=options,
            return_type=return_type,
            return_fields=None,
        )


def test_search_query_port_structural() -> None:
    """Implementations satisfy the protocol structurally."""
    stub: SearchQueryPort[_Hit] = _StubSearch()
    assert stub is not None


@pytest.mark.asyncio
async def test_search_default_projection() -> None:
    stub = _StubSearch()
    page = await stub.search_page("q")
    assert page.count == 1
    assert page.hits[0].id == "1"


@pytest.mark.asyncio
async def test_search_return_type_projection() -> None:
    stub = _StubSearch()
    page = await stub.select_search_page(_Alt, "q")
    assert page.count == 1
    assert page.hits[0].name == "x"


@pytest.mark.asyncio
async def test_search_return_fields_json() -> None:
    stub = _StubSearch()
    page = await stub.project_search_page(["id"], "q")
    assert page.count == 1
    assert page.hits[0]["id"] == "1"


@pytest.mark.asyncio
async def test_search_accepts_query_sequence() -> None:
    stub = _StubSearch()
    page = await stub.search_page(["a", "b"])
    assert page.count == 1
    assert page.hits[0].id == "1"
