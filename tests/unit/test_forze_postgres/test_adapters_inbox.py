"""Unit tests for PostgresInboxStore (mocked client)."""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.tenancy import TenantIdentity
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


async def test_mark_if_unseen_scopes_route_by_tenant_when_aware() -> None:
    tenant_id = uuid4()
    client = AsyncMock()
    client.execute = AsyncMock(return_value=1)
    store = PostgresInboxStore(
        client=client,
        spec=InboxSpec(name="events"),
        config=PostgresInboxConfig(relation=("public", "inbox"), tenant_aware=True),
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tenant_id),
    )

    assert await store.mark_if_unseen("events", "m1") is True

    params = client.execute.await_args.args[1]
    assert params[0] == f"t:{tenant_id}:events"
    assert params[1] == "m1"
