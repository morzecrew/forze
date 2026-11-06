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
    ReadOnlyAware,
    TransactionManagerPort,
)
from forze.application.execution.context.transaction import TransactionContext
from forze.base.exceptions import CoreException
from forze_neo4j.adapters import Neo4jTxManagerAdapter, Neo4jTxScopeKey

pytestmark = pytest.mark.unit


@attrs.define(slots=True)
class _FakeClient:
    """Records transaction enter/commit/rollback (a rollback = the body raised)."""

    entered: int = 0
    committed: int = 0
    rolled_back: int = 0
    depth: int = 0

    def is_in_transaction(self) -> bool:
        return self.depth > 0

    @contextlib.asynccontextmanager
    async def transaction(self, *, database: str | None = None) -> AsyncGenerator[None]:
        _ = database
        self.entered += 1
        self.depth += 1
        try:
            yield
        except BaseException:
            self.rolled_back += 1
            raise
        else:
            self.committed += 1
        finally:
            self.depth -= 1


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
    # No read-only mode wired: a read-only root is a hint, so it still marks a commit point.
    mgr = Neo4jTxManagerAdapter(client=_FakeClient())  # type: ignore[arg-type]
    assert not isinstance(mgr, ReadOnlyAware)


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


@pytest.mark.asyncio
async def test_rollback_only_rolls_back_the_root_and_is_refused_nested() -> None:
    """Nested Neo4j scopes reuse the root's transaction, so only the root can roll back."""

    client = _FakeClient()
    tx = TransactionContext()
    tx.lock(lambda _route: Neo4jTxManagerAdapter(client=client))  # type: ignore[arg-type]

    async with tx.scope("graph") as scope:
        scope.rollback_only()

    assert (client.entered, client.committed, client.rolled_back) == (1, 0, 1)

    with pytest.raises(CoreException) as caught:
        async with tx.scope("graph"), tx.scope("graph") as nested:
            nested.rollback_only()

    assert caught.value.code == "tx_nested_rollback_unsupported"


@pytest.mark.asyncio
async def test_a_scope_joins_only_inside_its_clients_transaction() -> None:
    mgr = Neo4jTxManagerAdapter(client=_FakeClient())  # type: ignore[arg-type]
    assert not mgr.is_transactionally_enlisted()

    async with mgr.transaction():
        assert mgr.is_transactionally_enlisted()
