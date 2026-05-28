"""Tests for forze.application.contracts.search.ports (SearchCommandPort)."""

from __future__ import annotations

from collections.abc import Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchCommandPort


class _Doc(BaseModel):
    id: str
    title: str = ""


class _StubSearchCommand:
    def __init__(self) -> None:
        self.ensure_calls = 0
        self.upserts: list[Sequence[_Doc]] = []
        self.deletes: list[Sequence[str]] = []
        self.delete_all_calls = 0

    async def ensure_index(self) -> None:
        self.ensure_calls += 1

    async def upsert(self, documents: Sequence[_Doc]) -> None:
        self.upserts.append(tuple(documents))

    async def upsert_many(self, documents: Sequence[_Doc]) -> None:
        self.upserts.append(tuple(documents))

    async def delete(self, ids: Sequence[str]) -> None:
        self.deletes.append(tuple(ids))

    async def delete_all(self) -> None:
        self.delete_all_calls += 1


def test_search_command_port_structural() -> None:
    stub: SearchCommandPort[_Doc] = _StubSearchCommand()
    assert stub is not None


@pytest.mark.asyncio
async def test_search_command_port_methods() -> None:
    stub = _StubSearchCommand()
    await stub.ensure_index()
    await stub.upsert([_Doc(id="1", title="a")])
    await stub.upsert_many([_Doc(id="2")])
    await stub.delete(["1"])
    await stub.delete_all()
    assert stub.ensure_calls == 1
    assert len(stub.upserts) == 2
    assert stub.deletes == [("1",)]
    assert stub.delete_all_calls == 1
