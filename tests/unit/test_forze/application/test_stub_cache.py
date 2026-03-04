"""Unit tests for InMemoryCachePort stub."""

import pytest

from ._stubs import InMemoryCachePort

# ----------------------- #


class TestInMemoryCachePort:
    """Tests for InMemoryCachePort stub."""

    @pytest.mark.asyncio
    async def test_get_missing_returns_none(self) -> None:
        port = InMemoryCachePort()
        assert await port.get("missing") is None

    @pytest.mark.asyncio
    async def test_set_and_get(self) -> None:
        port = InMemoryCachePort()
        await port.set("k1", "v1")
        assert await port.get("k1") == "v1"

    @pytest.mark.asyncio
    async def test_set_overwrites(self) -> None:
        port = InMemoryCachePort()
        await port.set("k", "v1")
        await port.set("k", "v2")
        assert await port.get("k") == "v2"

    @pytest.mark.asyncio
    async def test_get_many_returns_found_and_missing(self) -> None:
        port = InMemoryCachePort()
        await port.set("a", 1)
        await port.set("c", 3)
        found, missing = await port.get_many(["a", "b", "c"])
        assert found == {"a": 1, "c": 3}
        assert set(missing) == {"b"}

    @pytest.mark.asyncio
    async def test_set_many(self) -> None:
        port = InMemoryCachePort()
        await port.set_many({"x": 1, "y": 2})
        assert await port.get("x") == 1
        assert await port.get("y") == 2

    @pytest.mark.asyncio
    async def test_set_versioned(self) -> None:
        port = InMemoryCachePort()
        await port.set_versioned("k", "v1", "data1")
        await port.set_versioned("k", "v2", "data2")
        assert await port.get("k") == "data2"

    @pytest.mark.asyncio
    async def test_delete_removes_key(self) -> None:
        port = InMemoryCachePort()
        await port.set("k", "v")
        await port.delete("k", hard=True)
        assert await port.get("k") is None

    @pytest.mark.asyncio
    async def test_delete_many(self) -> None:
        port = InMemoryCachePort()
        await port.set_many({"a": 1, "b": 2, "c": 3})
        await port.delete_many(["a", "c"], hard=True)
        assert await port.get("a") is None
        assert await port.get("b") == 2
        assert await port.get("c") is None
