"""Firestore tx manager honors the kernel isolation contract (always serializable)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from forze.application.contracts.transaction import IsolationAware, IsolationLevel, ReadOnlyAware
from forze_firestore.adapters.txmanager import FirestoreTxManagerAdapter

# ----------------------- #


class _FakeClient:
    def __init__(self) -> None:
        self.entered = 0
        self.depth = 0

    def is_in_transaction(self) -> bool:
        return self.depth > 0

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[None]:
        self.entered += 1
        self.depth += 1

        try:
            yield

        finally:
            self.depth -= 1


def _adapter() -> tuple[FirestoreTxManagerAdapter, _FakeClient]:
    client = _FakeClient()
    return FirestoreTxManagerAdapter(client=client), client  # type: ignore[arg-type]


async def test_a_scope_joins_only_inside_its_clients_transaction() -> None:
    adapter, _ = _adapter()
    assert not adapter.is_transactionally_enlisted()

    async with adapter.transaction():
        assert adapter.is_transactionally_enlisted()


def test_reports_all_levels_serializable_satisfies_any() -> None:
    adapter, _ = _adapter()
    assert isinstance(adapter, IsolationAware)
    assert adapter.capabilities().isolation == frozenset(IsolationLevel)
    assert not adapter.capabilities().savepoints  # nested scopes share the root's transaction
    assert not isinstance(adapter, ReadOnlyAware)  # no read-only mode: a hint, so it marks


async def test_any_isolation_runs_a_serializable_transaction() -> None:
    for level in IsolationLevel:
        adapter, client = _adapter()
        async with adapter.transaction(isolation=level):
            pass
        assert client.entered == 1
