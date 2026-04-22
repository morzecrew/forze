"""Tests for forze.application.contracts.search.ports (SearchQueryPort)."""

from __future__ import annotations

from collections.abc import Sequence

import pytest
from pydantic import BaseModel

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
    ):
        _ = query, filters, pagination, sorts, options
        if return_fields is not None:
            return ([{"id": "1"}], 1)
        if return_type is not None:
            return ([return_type(name="x")], 1)
        return ([_Hit(id="1")], 1)


def test_search_query_port_structural() -> None:
    """Implementations satisfy the protocol structurally."""
    stub: SearchQueryPort[_Hit] = _StubSearch()
    assert stub is not None


@pytest.mark.asyncio
async def test_search_default_projection() -> None:
    stub = _StubSearch()
    hits, total = await stub.search("q")
    assert total == 1
    assert hits[0].id == "1"


@pytest.mark.asyncio
async def test_search_return_type_projection() -> None:
    stub = _StubSearch()
    hits, total = await stub.search("q", return_type=_Alt)
    assert total == 1
    assert hits[0].name == "x"


@pytest.mark.asyncio
async def test_search_return_fields_json() -> None:
    stub = _StubSearch()
    hits, total = await stub.search("q", return_fields=["id"])
    assert total == 1
    assert hits[0]["id"] == "1"


@pytest.mark.asyncio
async def test_search_accepts_query_sequence() -> None:
    stub = _StubSearch()
    hits, total = await stub.search(["a", "b"])
    assert total == 1
    assert hits[0].id == "1"
