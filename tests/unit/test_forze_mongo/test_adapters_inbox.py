"""Unit tests for :class:`MongoInboxStore` (mocked client)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze_mongo.adapters.inbox import MongoInboxStore
from forze_mongo.execution.deps.configs import MongoInboxConfig

# ----------------------- #

_TENANT = UUID("00000000-0000-0000-0000-000000000001")


def _store(
    *,
    upserted_id: str | None,
    tenant: UUID | None = None,
) -> tuple[MongoInboxStore, AsyncMock]:
    client = AsyncMock()
    client.update_one_upsert = AsyncMock(
        return_value=SimpleNamespace(upserted_id=upserted_id),
    )
    tenant_aware = tenant is not None
    store = MongoInboxStore(
        client=client,
        spec=InboxSpec(name="events"),
        config=MongoInboxConfig(collection=("app", "inbox"), tenant_aware=tenant_aware),
        tenant_aware=tenant_aware,
        tenant_provider=(lambda: TenantIdentity(tenant_id=tenant)) if tenant else (lambda: None),
    )
    return store, client


# ----------------------- #


@pytest.mark.asyncio
async def test_mark_if_unseen_true_when_inserted() -> None:
    store, client = _store(upserted_id="k")

    assert await store.mark_if_unseen("events", "m1") is True

    flt, update = client.update_one_upsert.await_args.args[1:3]
    assert flt == {"_id": "6:events|m1"}
    on_insert = update["$setOnInsert"]
    assert on_insert["inbox_route"] == "events"
    assert on_insert["message_id"] == "m1"
    assert on_insert["tenant_id"] is None


@pytest.mark.asyncio
async def test_mark_if_unseen_false_when_already_seen() -> None:
    store, _ = _store(upserted_id=None)

    assert await store.mark_if_unseen("events", "m1") is False


@pytest.mark.asyncio
async def test_doc_id_is_unambiguous_for_separator_contents() -> None:
    store, _ = _store(upserted_id=None)

    # Without the length prefix these two pairs would collide on "a|b|c".
    assert store._doc_id("a|b", "c") != store._doc_id("a", "b|c")


@pytest.mark.asyncio
async def test_tenant_tag_lands_in_key_and_fields() -> None:
    store, client = _store(upserted_id="k", tenant=_TENANT)

    assert await store.mark_if_unseen("events", "m1") is True

    flt, update = client.update_one_upsert.await_args.args[1:3]
    assert flt["_id"].startswith(f"tenant:{_TENANT}|")
    assert update["$setOnInsert"]["tenant_id"] == str(_TENANT)


def test_enlistment_reports_client_transaction_state() -> None:
    store, client = _store(upserted_id=None)
    client.is_in_transaction = MagicMock(return_value=True)

    assert store.is_transactionally_enlisted() is True

    client.is_in_transaction = MagicMock(return_value=False)
    assert store.is_transactionally_enlisted() is False
