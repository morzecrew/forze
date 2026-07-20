"""Postgres tx manager honors the kernel isolation contract (IsolationAware)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from forze.application.contracts.transaction import IsolationAware, IsolationLevel
from forze_postgres.adapters.txmanager import PostgresTxManagerAdapter
from forze_postgres.kernel.client import PostgresTransactionOptions

# ----------------------- #


class _FakeClient:
    def __init__(self) -> None:
        self.options: PostgresTransactionOptions | None = None

    def is_in_transaction(self) -> bool:
        return False

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
