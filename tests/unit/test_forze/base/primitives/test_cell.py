"""Unit tests for :class:`forze.base.primitives.OnceCell`."""

from __future__ import annotations

import attrs
import pytest

from forze.base.primitives import OnceCell

# ----------------------- #


def test_peek_is_none_until_set() -> None:
    cell: OnceCell[int] = OnceCell()
    assert cell.peek() is None
    assert cell.set(7) == 7
    assert cell.peek() == 7


def test_get_or_compute_runs_factory_once() -> None:
    cell: OnceCell[int] = OnceCell()
    calls = [0]

    def factory() -> int:
        calls[0] += 1
        return 42

    assert cell.get_or_compute(factory) == 42
    assert cell.get_or_compute(factory) == 42
    assert calls[0] == 1


def test_get_or_compute_caches_falsey_bool() -> None:
    cell: OnceCell[bool] = OnceCell()
    calls = [0]

    def factory() -> bool:
        calls[0] += 1
        return False

    assert cell.get_or_compute(factory) is False
    assert cell.get_or_compute(factory) is False
    assert calls[0] == 1  # False is a set value, not recomputed


@pytest.mark.asyncio
async def test_resolve_memoizes_when_cache_true() -> None:
    cell: OnceCell[str] = OnceCell()
    calls = [0]

    async def factory() -> str:
        calls[0] += 1
        return f"v{calls[0]}"

    assert await cell.resolve(factory, cache=True) == "v1"
    assert await cell.resolve(factory, cache=True) == "v1"
    assert calls[0] == 1
    assert cell.peek() == "v1"


@pytest.mark.asyncio
async def test_resolve_fresh_when_cache_false() -> None:
    cell: OnceCell[str] = OnceCell()
    calls = [0]

    async def factory() -> str:
        calls[0] += 1
        return f"v{calls[0]}"

    assert await cell.resolve(factory, cache=False) == "v1"
    assert await cell.resolve(factory, cache=False) == "v2"
    assert calls[0] == 2
    assert cell.peek() is None  # nothing memoized


def test_cell_is_per_instance_on_frozen_owner() -> None:
    @attrs.define(slots=True, frozen=True)
    class Owner:
        _cell: OnceCell[int] = attrs.field(factory=OnceCell, init=False)

    a, b = Owner(), Owner()
    a._cell.set(1)
    assert a._cell.peek() == 1
    assert b._cell.peek() is None  # independent cells
