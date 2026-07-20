"""Unit tests for the Neo4j transaction manager adapter."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncGenerator

import attrs
import pytest

pytest.importorskip("neo4j")

from forze.application.contracts.transaction import (
    IsolationAware,
    IsolationLevel,
    TransactionManagerPort,
)
from forze_neo4j.adapters import Neo4jTxManagerAdapter, Neo4jTxScopeKey

pytestmark = pytest.mark.unit


@attrs.define(slots=True)
class _FakeClient:
    """Records transaction enter/commit/rollback (a rollback = the body raised)."""

    entered: int = 0
    committed: int = 0
    rolled_back: int = 0

    @contextlib.asynccontextmanager
    async def transaction(self, *, database: str | None = None) -> AsyncGenerator[None]:
        _ = database
        self.entered += 1
        try:
            yield
        except BaseException:
            self.rolled_back += 1
            raise
        else:
            self.committed += 1


def test_is_transaction_manager_and_isolation_aware() -> None:
    mgr = Neo4jTxManagerAdapter(client=_FakeClient())  # type: ignore[arg-type]
    assert isinstance(mgr, TransactionManagerPort)
    assert isinstance(mgr, IsolationAware)
    assert mgr.scope_key is Neo4jTxScopeKey


def test_capabilities_declare_read_committed_only() -> None:
    """Neo4j runs at READ COMMITTED and cannot switch — only that level is advertised."""

    caps = Neo4jTxManagerAdapter(client=_FakeClient()).capabilities()  # type: ignore[arg-type]
    assert caps.isolation == frozenset({IsolationLevel.READ_COMMITTED})
    assert IsolationLevel.SERIALIZABLE not in caps.isolation


@pytest.mark.asyncio
async def test_transaction_commits_on_success() -> None:
    client = _FakeClient()
    mgr = Neo4jTxManagerAdapter(client=client)  # type: ignore[arg-type]

    async with mgr.transaction():
        pass

    assert (client.entered, client.committed, client.rolled_back) == (1, 1, 0)


@pytest.mark.asyncio
async def test_transaction_rolls_back_on_error() -> None:
    client = _FakeClient()
    mgr = Neo4jTxManagerAdapter(client=client)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        async with mgr.transaction():
            raise ValueError("boom")

    assert (client.entered, client.committed, client.rolled_back) == (1, 0, 1)
