"""Unit tests for InMemorySearchReadPort stub."""

import pytest
from pydantic import BaseModel

from ._stubs import InMemorySearchReadPort

# ----------------------- #


class TestInMemorySearchReadPort:
    """Tests for InMemorySearchReadPort stub."""

    @pytest.mark.asyncio
    async def test_search_empty_returns_empty(self) -> None:
        port = InMemorySearchReadPort()
        hits, count = await port.search("q")
        assert hits == []
        assert count == 0

    @pytest.mark.asyncio
    async def test_add_hits_returns_seeded(self) -> None:
        port = InMemorySearchReadPort()
        port.add_hits("foo", [{"id": "1"}, {"id": "2"}])
        hits, count = await port.search("foo")
        assert count == 2
        assert hits[0]["id"] == "1"
        assert hits[1]["id"] == "2"

    @pytest.mark.asyncio
    async def test_set_default_hits(self) -> None:
        port = InMemorySearchReadPort()
        port.set_default_hits([{"x": 1}])
        hits, count = await port.search("unknown")
        assert count == 1
        assert hits[0]["x"] == 1

    @pytest.mark.asyncio
    async def test_search_respects_limit(self) -> None:
        port = InMemorySearchReadPort()
        port.add_hits("q", [{"a": i} for i in range(5)])
        hits, count = await port.search("q", limit=2)
        assert count == 5
        assert len(hits) == 2

    @pytest.mark.asyncio
    async def test_search_respects_offset(self) -> None:
        port = InMemorySearchReadPort()
        port.add_hits("q", [{"a": i} for i in range(5)])
        hits, count = await port.search("q", offset=2, limit=2)
        assert count == 5
        assert len(hits) == 2
        assert hits[0]["a"] == 2
        assert hits[1]["a"] == 3

    @pytest.mark.asyncio
    async def test_search_with_return_fields_projects(self) -> None:
        port = InMemorySearchReadPort()
        port.add_hits("q", [{"id": "1", "title": "a", "extra": "x"}])
        hits, count = await port.search("q", return_fields=["id", "title"])
        assert hits[0] == {"id": "1", "title": "a"}

    @pytest.mark.asyncio
    async def test_search_with_typed_hits(self) -> None:
        class Hit(BaseModel):
            id: str
            name: str

        port = InMemorySearchReadPort()
        port.add_hits("q", [Hit(id="1", name="first")])
        hits, count = await port.search("q")
        assert count == 1
        assert hits[0].id == "1"
        assert hits[0].name == "first"
