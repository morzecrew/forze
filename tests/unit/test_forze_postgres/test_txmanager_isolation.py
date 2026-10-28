"""Postgres tx manager honors the kernel isolation contract (IsolationAware)."""

from __future__ import annotations

import contextvars
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytest

from forze.application.contracts.transaction import (
    IsolationAware,
    IsolationLevel,
    ReadOnlyAware,
)
from forze.application.execution.context.commit_state import (
    commit_started,
    reset_commit_started,
)
from forze.application.execution.context.transaction import TransactionContext
from forze_postgres.adapters.txmanager import PostgresTxManagerAdapter
from forze_postgres.kernel.client import PostgresClient
from forze_postgres.kernel.client import PostgresTransactionOptions
from forze_postgres.kernel.client.read_only_root import (
    read_only_root,
    read_only_root_enforced,
)

# ----------------------- #


class _FakeClient:
    def __init__(self) -> None:
        self.options: PostgresTransactionOptions | None = None
        self.in_transaction = False

    def is_in_transaction(self) -> bool:
        return self.in_transaction

    def deadline_pushdown(self) -> None:
        return None  # push-down disabled for this isolation-only stub

    @asynccontextmanager
    async def transaction(
        self, *, options: PostgresTransactionOptions
    ) -> AsyncGenerator[None]:
        self.options = options
        yield


def _adapter() -> tuple[PostgresTxManagerAdapter, _FakeClient]:
    client = _FakeClient()
    return PostgresTxManagerAdapter(client=client), client  # type: ignore[arg-type]


def test_is_isolation_aware_and_reports_all_levels() -> None:
    adapter, _ = _adapter()
    assert isinstance(adapter, IsolationAware)
    assert adapter.capabilities().isolation == frozenset(IsolationLevel)


@pytest.mark.parametrize(
    ("read_only", "in_outer_transaction", "enforced"),
    [
        pytest.param(True, False, True, id="read-only-begin"),
        pytest.param(True, True, False, id="savepoint"),
        pytest.param(False, False, False, id="read-write"),
    ],
)
async def test_read_only_is_enforced_only_by_a_read_only_begin(
    read_only: bool, in_outer_transaction: bool, enforced: bool
) -> None:
    # Inside an outer client transaction the root is a savepoint: it cannot be READ ONLY,
    # and the outer transaction may write.
    adapter, client = _adapter()
    client.in_transaction = in_outer_transaction
    assert isinstance(adapter, ReadOnlyAware)

    async with adapter.transaction(read_only=read_only):
        assert adapter.read_only_enforced() is enforced

        async with adapter.transaction():  # a nested scope does not change the root's answer
            pass

        assert adapter.read_only_enforced() is enforced

    assert adapter.read_only_enforced() is False


@pytest.mark.parametrize(
    ("in_outer_transaction", "detached", "marked"),
    [
        pytest.param(False, False, False, id="read-only-begin"),
        pytest.param(True, False, True, id="savepoint"),
        pytest.param(False, True, True, id="detached-statement"),
    ],
)
async def test_a_read_only_root_marks_the_commit_point_unless_enforced(
    in_outer_transaction: bool, detached: bool, marked: bool
) -> None:
    adapter, client = _adapter()
    client.in_transaction = in_outer_transaction
    tx = TransactionContext()
    tx.lock(lambda _route: adapter)
    reset_commit_started()

    async with tx.scope("pg", read_only=True):
        if detached:
            # Statements in a detached scope autocommit outside the root, on any client.
            async with PostgresClient().detached():
                pass

    assert commit_started() is marked
    reset_commit_started()


class _FakeCursor:
    rowcount = 0

    async def __aenter__(self) -> "_FakeCursor":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def execute(self, *_args: object) -> None:
        return None


class _FakeConnection:
    async def set_autocommit(self, _on: bool) -> None:
        return None

    def cursor(self) -> _FakeCursor:
        return _FakeCursor()


async def test_an_autocommit_statement_on_another_client_keeps_the_mark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A second client outside any transaction runs the statement in autocommit: it commits
    # beside the read-only root, so the root is no longer the only thing that committed.
    @asynccontextmanager
    async def _pooled(_self: object) -> AsyncGenerator[_FakeConnection]:
        yield _FakeConnection()

    monkeypatch.setattr(PostgresClient, "_acquire", _pooled)
    adapter, _client = _adapter()
    tx = TransactionContext()
    tx.lock(lambda _route: adapter)
    reset_commit_started()

    async with tx.scope("pg", read_only=True):
        await PostgresClient().execute("SELECT 1")

    assert commit_started() is True
    reset_commit_started()


async def test_a_bound_connection_on_another_client_keeps_the_mark(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The pool commits a bound connection's statements when the binding ends.
    @asynccontextmanager
    async def _pooled(_self: object) -> AsyncGenerator[_FakeConnection]:
        yield _FakeConnection()

    monkeypatch.setattr(PostgresClient, "_acquire", _pooled)
    adapter, _client = _adapter()
    tx = TransactionContext()
    tx.lock(lambda _route: adapter)
    reset_commit_started()

    async with tx.scope("pg", read_only=True):
        async with PostgresClient().bound_connection():
            pass

    assert commit_started() is True
    reset_commit_started()


async def test_a_detached_scope_clears_the_claim_even_from_a_nested_scope() -> None:
    adapter, _client = _adapter()

    async with adapter.transaction(read_only=True):
        async with adapter.transaction():  # a savepoint shares the root's record
            async with PostgresClient().detached():
                pass

        assert adapter.read_only_enforced() is False


async def test_a_bypass_in_a_nested_read_only_root_clears_the_outer_claim() -> None:
    outer, _ = _adapter()
    inner, _ = _adapter()

    async with outer.transaction(read_only=True):
        async with inner.transaction(read_only=True):  # another client's own root
            async with PostgresClient().detached():
                pass

        assert outer.read_only_enforced() is False


def test_a_record_finalized_in_another_context_is_dropped_there() -> None:
    # An abandoned generator can be finalized outside the context that entered the record.
    entered = contextvars.copy_context()
    record = read_only_root()
    entered.run(record.__enter__)
    finalizer = entered.copy()

    finalizer.run(record.__exit__, None, None, None)

    assert finalizer.run(read_only_root_enforced) is False
    assert entered.run(read_only_root_enforced) is True


async def test_isolation_maps_to_postgres_level() -> None:
    cases = {
        IsolationLevel.READ_COMMITTED: "read_committed",
        IsolationLevel.SNAPSHOT: "repeatable_read",
        IsolationLevel.SERIALIZABLE: "serializable",
    }
    for level, expected in cases.items():
        adapter, client = _adapter()
        async with adapter.transaction(isolation=level):
            pass
        assert client.options is not None
        assert client.options.isolation == expected


async def test_no_isolation_leaves_the_default() -> None:
    adapter, client = _adapter()
    async with adapter.transaction():
        pass
    assert client.options is not None
    assert client.options.isolation == "read_committed"  # the options default
