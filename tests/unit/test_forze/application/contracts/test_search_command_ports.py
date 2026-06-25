"""Tests for forze.application.contracts.search.ports — the command/management split."""

from __future__ import annotations

from collections.abc import Sequence

import pytest
from pydantic import BaseModel

from forze.application.contracts.search import SearchCommandPort, SearchManagementPort


class _Doc(BaseModel):
    id: str
    title: str = ""


class _StubSearchCommand:
    """Data-plane only: upsert / delete (no ensure_index / delete_all)."""

    def __init__(self) -> None:
        self.upserts: list[Sequence[_Doc]] = []
        self.deletes: list[Sequence[str]] = []

    async def upsert(self, documents: Sequence[_Doc]) -> None:
        self.upserts.append(tuple(documents))

    async def upsert_many(self, documents: Sequence[_Doc]) -> None:
        self.upserts.append(tuple(documents))

    async def delete(self, ids: Sequence[str]) -> None:
        self.deletes.append(tuple(ids))


class _StubSearchManagement:
    """Control-plane only: ensure_index / delete_all."""

    def __init__(self) -> None:
        self.ensure_calls = 0
        self.delete_all_calls = 0

    async def ensure_index(self) -> None:
        self.ensure_calls += 1

    async def delete_all(self) -> None:
        self.delete_all_calls += 1


def test_ports_are_structurally_distinct() -> None:
    cmd: SearchCommandPort[_Doc] = _StubSearchCommand()
    mgmt: SearchManagementPort = _StubSearchManagement()
    assert cmd is not None and mgmt is not None


@pytest.mark.asyncio
async def test_search_command_port_methods() -> None:
    stub = _StubSearchCommand()
    await stub.upsert([_Doc(id="1", title="a")])
    await stub.upsert_many([_Doc(id="2")])
    await stub.delete(["1"])
    assert len(stub.upserts) == 2
    assert stub.deletes == [("1",)]


@pytest.mark.asyncio
async def test_search_management_port_methods() -> None:
    stub = _StubSearchManagement()
    await stub.ensure_index()
    await stub.delete_all()
    assert stub.ensure_calls == 1
    assert stub.delete_all_calls == 1
