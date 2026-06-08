"""Unit tests for PostgresInboxStore (mocked client)."""

from __future__ import annotations

from unittest.mock import AsyncMock

from forze.application.contracts.inbox import InboxSpec
from forze_postgres.adapters.inbox import PostgresInboxStore
from forze_postgres.execution.deps.configs import PostgresInboxConfig

# ----------------------- #


def _store(rowcount: int) -> tuple[PostgresInboxStore, AsyncMock]:
    client = AsyncMock()
    client.execute = AsyncMock(return_value=rowcount)
    store = PostgresInboxStore(
        client=client,
        spec=InboxSpec(name="events"),
        config=PostgresInboxConfig(relation=("public", "inbox"), tenant_aware=False),
        tenant_aware=False,
        tenant_provider=lambda: None,
    )
    return store, client


async def test_mark_if_unseen_true_when_inserted() -> None:
    store, client = _store(1)

    assert await store.mark_if_unseen("events", "m1") is True

    params = client.execute.await_args.args[1]
    assert params[0] == "events"
    assert params[1] == "m1"
    assert client.execute.await_args.kwargs["return_rowcount"] is True


async def test_mark_if_unseen_false_on_conflict() -> None:
    store, _ = _store(0)

    assert await store.mark_if_unseen("events", "m1") is False
