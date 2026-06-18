"""Firestore tx manager honors the kernel isolation contract (always serializable)."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from forze.application.contracts.transaction import IsolationAware, IsolationLevel
from forze_firestore.adapters.txmanager import FirestoreTxManagerAdapter

# ----------------------- #


class _FakeClient:
    def __init__(self) -> None:
        self.entered = 0

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        self.entered += 1
        yield


def _adapter() -> tuple[FirestoreTxManagerAdapter, _FakeClient]:
    client = _FakeClient()
    return FirestoreTxManagerAdapter(client=client), client  # type: ignore[arg-type]


def test_reports_all_levels_serializable_satisfies_any() -> None:
    adapter, _ = _adapter()
    assert isinstance(adapter, IsolationAware)
    assert adapter.capabilities().isolation == frozenset(IsolationLevel)


async def test_any_isolation_runs_a_serializable_transaction() -> None:
    for level in IsolationLevel:
        adapter, client = _adapter()
        async with adapter.transaction(isolation=level):
            pass
        assert client.entered == 1
