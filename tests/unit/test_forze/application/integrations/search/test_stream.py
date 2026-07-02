"""Bounded-memory search streaming loop + mixin wiring (RFC 0011)."""

from __future__ import annotations

from typing import Any, Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.querying import CursorPaginationExpression
from forze.application.contracts.search import SearchCapabilities, SearchCursorPage
from forze.application.integrations.search import (
    SimpleSearchPortMixin,
    stream_search_pages,
)
from forze.base.exceptions import CoreException, ExceptionKind

pytestmark = pytest.mark.unit


class _Doc(BaseModel):
    id: str


def _page(
    hits: Sequence[Any], *, next_cursor: str | None, has_more: bool
) -> SearchCursorPage[Any]:
    return SearchCursorPage(
        hits=list(hits), next_cursor=next_cursor, prev_cursor=None, has_more=has_more
    )


# ....................... #


class TestStreamSearchPages:
    @pytest.mark.asyncio
    async def test_pages_until_exhausted(self) -> None:
        items = [_Doc(id=str(i)) for i in range(5)]

        async def fetch(cursor: CursorPaginationExpression) -> SearchCursorPage[Any]:
            start = int(cursor.get("after") or 0)  # type: ignore[arg-type]
            limit = int(cursor.get("limit") or 10)  # type: ignore[arg-type]
            window = items[start : start + limit]
            nxt = str(start + limit) if start + limit < len(items) else None
            return _page(window, next_cursor=nxt, has_more=nxt is not None)

        chunks = [c async for c in stream_search_pages(fetch, chunk_size=2)]
        assert [len(c) for c in chunks] == [2, 2, 1]

    @pytest.mark.asyncio
    async def test_chunk_size_must_be_positive(self) -> None:
        async def fetch(_c: CursorPaginationExpression) -> SearchCursorPage[Any]:
            return _page([], next_cursor=None, has_more=False)

        with pytest.raises(CoreException):
            [c async for c in stream_search_pages(fetch, chunk_size=0)]

    @pytest.mark.asyncio
    async def test_non_advancing_cursor_raises(self) -> None:
        async def fetch(_c: CursorPaginationExpression) -> SearchCursorPage[Any]:
            return _page([_Doc(id="x")], next_cursor="stuck", has_more=True)

        with pytest.raises(CoreException, match="did not advance") as ei:
            [c async for c in stream_search_pages(fetch, chunk_size=1)]
        assert ei.value.kind is ExceptionKind.INTERNAL

    @pytest.mark.asyncio
    async def test_max_pages_guard(self) -> None:
        counter = {"n": 0}

        async def fetch(_c: CursorPaginationExpression) -> SearchCursorPage[Any]:
            counter["n"] += 1
            tok = str(counter["n"])
            return _page([_Doc(id=tok)], next_cursor=tok, has_more=True)

        with pytest.raises(CoreException, match="max_pages") as ei:
            [c async for c in stream_search_pages(fetch, chunk_size=1, max_pages=2)]
        assert ei.value.kind is ExceptionKind.PRECONDITION


# ....................... #


class _MixinAdapter(SimpleSearchPortMixin[_Doc]):
    """Minimal keyset adapter exercising the mixin's ``*_stream`` delegation."""

    def __init__(self, items: list[_Doc], *, supports_stream: bool = True) -> None:
        self._items = items
        self._supports = supports_stream

    @property
    def search_capabilities(self) -> SearchCapabilities:
        return SearchCapabilities(supports_stream=self._supports)

    async def _cursor_search_impl(  # type: ignore[override]
        self,
        query: Any,
        filters: Any = None,
        cursor: Any = None,
        sorts: Any = None,
        *,
        options: Any = None,
        return_type: Any = None,
        return_fields: Any = None,
    ) -> SearchCursorPage[Any]:
        cur = dict(cursor or {})
        start = int(cur.get("after") or 0)
        limit = int(cur.get("limit") or 10)
        window = self._items[start : start + limit]
        nxt = str(start + limit) if start + limit < len(self._items) else None

        if return_fields is not None:
            hits: list[Any] = [{f: getattr(it, f) for f in return_fields} for it in window]
        elif return_type is not None:
            hits = [return_type(id=it.id) for it in window]
        else:
            hits = list(window)

        return _page(hits, next_cursor=nxt, has_more=nxt is not None)


class TestMixinStream:
    @pytest.mark.asyncio
    async def test_all_three_variants_delegate(self) -> None:
        items = [_Doc(id=str(i)) for i in range(7)]
        adapter = _MixinAdapter(items)

        got = [h async for c in adapter.search_stream("q", chunk_size=3) for h in c]
        assert [d.id for d in got] == [str(i) for i in range(7)]

        proj = [
            r
            async for c in adapter.project_search_stream(["id"], "q", chunk_size=3)
            for r in c
        ]
        assert len(proj) == 7
        assert all(set(r.keys()) == {"id"} for r in proj)

        sel = [
            r
            async for c in adapter.select_search_stream(_Doc, "q", chunk_size=3)
            for r in c
        ]
        assert len(sel) == 7
        assert all(isinstance(r, _Doc) for r in sel)

    @pytest.mark.asyncio
    async def test_fails_closed_when_capability_off(self) -> None:
        adapter = _MixinAdapter([_Doc(id="1")], supports_stream=False)
        with pytest.raises(CoreException, match="result streaming"):
            async for _ in adapter.search_stream("q", chunk_size=2):
                pass
