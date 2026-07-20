"""Mongo tx manager honors the kernel isolation contract (snapshot; no serializable)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from forze.application.contracts.transaction import IsolationAware, IsolationLevel
from forze_mongo.adapters.txmanager import MongoTxManagerAdapter
from forze_mongo.kernel.client import MongoTransactionOptions

# ----------------------- #


class _FakeClient:
    def __init__(self) -> None:
        self.options: MongoTransactionOptions | None = None

    @asynccontextmanager
    async def transaction(
        self, *, options: MongoTransactionOptions
    ) -> AsyncGenerator[None]:
        self.options = options
        yield


def _adapter() -> tuple[MongoTxManagerAdapter, _FakeClient]:
    client = _FakeClient()
    return MongoTxManagerAdapter(client=client), client  # type: ignore[arg-type]


def test_reports_snapshot_but_not_serializable() -> None:
    adapter, _ = _adapter()
    assert isinstance(adapter, IsolationAware)
    caps = adapter.capabilities().isolation
    assert IsolationLevel.SNAPSHOT in caps
    assert IsolationLevel.READ_COMMITTED in caps
    assert IsolationLevel.SERIALIZABLE not in caps  # Mongo has no serializable mode


async def test_snapshot_sets_read_concern_snapshot() -> None:
    adapter, client = _adapter()
    async with adapter.transaction(isolation=IsolationLevel.SNAPSHOT):
        pass
    assert client.options is not None
    assert client.options.read_concern is not None
    assert client.options.read_concern.level == "snapshot"


async def test_read_committed_leaves_default_read_concern() -> None:
    adapter, client = _adapter()
    async with adapter.transaction(isolation=IsolationLevel.READ_COMMITTED):
        pass
    assert client.options is not None
    assert client.options.read_concern is None
