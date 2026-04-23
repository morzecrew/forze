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
    async def search(
        self,
        query: str | Sequence[str],
        filters=None,
        pagination=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields=None,
        return_count: bool = False,
    ):
        _ = query, filters, pagination, sorts, options
        if return_fields is not None:
            data: list[object] = [{"id": "1"}]
        elif return_type is not None:
            data = [return_type(name="x")]
        else:
            data = [_Hit(id="1")]
        if return_count:
            return page_from_limit_offset(data, pagination, total=1)
        return page_from_limit_offset(data, pagination, total=None)

    async def search_with_cursor(
        self,
        query: str | Sequence[str],
        filters=None,
        cursor=None,
        sorts=None,
        *,
        options: SearchOptions | None = None,
        return_type: type[BaseModel] | None = None,
        return_fields=None,
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


def test_search_query_port_structural() -> None:
    """Implementations satisfy the protocol structurally."""
    stub: SearchQueryPort[_Hit] = _StubSearch()
    assert stub is not None


@pytest.mark.asyncio
async def test_search_default_projection() -> None:
    stub = _StubSearch()
    page = await stub.search("q", return_count=True)
    assert page.count == 1
    assert page.hits[0].id == "1"


@pytest.mark.asyncio
async def test_search_return_type_projection() -> None:
    stub = _StubSearch()
    page = await stub.search("q", return_type=_Alt, return_count=True)
    assert page.count == 1
    assert page.hits[0].name == "x"


@pytest.mark.asyncio
async def test_search_return_fields_json() -> None:
    stub = _StubSearch()
    page = await stub.search("q", return_fields=["id"], return_count=True)
    assert page.count == 1
    assert page.hits[0]["id"] == "1"


@pytest.mark.asyncio
async def test_search_accepts_query_sequence() -> None:
    stub = _StubSearch()
    page = await stub.search(["a", "b"], return_count=True)
    assert page.count == 1
    assert page.hits[0].id == "1"
