"""Integration tests for the Mongo inbox (consumer-side dedup) adapter.

# covers: InboxPort.mark_if_unseen
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from forze.application.contracts.inbox import InboxSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze_mongo.adapters.inbox import MongoInboxStore
from forze_mongo.execution.deps.configs import MongoInboxConfig
from forze_mongo.kernel.client import MongoClient

# ----------------------- #

_TENANT_A = UUID("00000000-0000-0000-0000-00000000000a")
_TENANT_B = UUID("00000000-0000-0000-0000-00000000000b")


async def _store(
    client: MongoClient,
    *,
    tenant: UUID | None = None,
    coll_name: str | None = None,
) -> MongoInboxStore:
    db_name = (await client.db()).name
    tenant_aware = tenant is not None

    return MongoInboxStore(
        client=client,
        spec=InboxSpec(name="events"),
        config=MongoInboxConfig(
            collection=(db_name, coll_name or f"inbox_{uuid4().hex[:8]}"),
            tenant_aware=tenant_aware,
        ),
        tenant_aware=tenant_aware,
        tenant_provider=(lambda: TenantIdentity(tenant_id=tenant)) if tenant else (lambda: None),
    )


# ----------------------- #


@pytest.mark.asyncio
async def test_mark_if_unseen_dedups(mongo_client: MongoClient) -> None:
    """First mark records; the second and every later one report already-seen."""

    store = await _store(mongo_client)

    assert await store.mark_if_unseen("events", "m1") is True
    assert await store.mark_if_unseen("events", "m1") is False
    assert await store.mark_if_unseen("events", "m1") is False


@pytest.mark.asyncio
async def test_marks_are_scoped_per_inbox_route(mongo_client: MongoClient) -> None:
    """The same message id under two inbox routes is two independent marks."""

    store = await _store(mongo_client)

    assert await store.mark_if_unseen("events", "m1") is True
    assert await store.mark_if_unseen("audit", "m1") is True
    assert await store.mark_if_unseen("audit", "m1") is False


@pytest.mark.asyncio
async def test_marks_are_scoped_per_tenant(mongo_client: MongoClient) -> None:
    """Two tenants sharing one collection never dedup each other's messages."""

    coll_name = f"inbox_{uuid4().hex[:8]}"
    store_a = await _store(mongo_client, tenant=_TENANT_A, coll_name=coll_name)
    store_b = await _store(mongo_client, tenant=_TENANT_B, coll_name=coll_name)

    assert await store_a.mark_if_unseen("events", "m1") is True
    assert await store_b.mark_if_unseen("events", "m1") is True
    assert await store_a.mark_if_unseen("events", "m1") is False


@pytest.mark.asyncio
async def test_rolled_back_transaction_discards_the_mark(
    mongo_client_replica: MongoClient,
) -> None:
    """The mark commits with the handler's transaction: a rollback frees the message for
    redelivery (exactly-once effect, not at-most-once)."""

    store = await _store(mongo_client_replica)

    with pytest.raises(RuntimeError, match="rollback"):
        async with mongo_client_replica.transaction():
            assert await store.mark_if_unseen("events", "m1") is True
            raise RuntimeError("rollback")

    # The aborted mark is gone: redelivery processes the message.
    assert await store.mark_if_unseen("events", "m1") is True
    assert await store.mark_if_unseen("events", "m1") is False


@pytest.mark.asyncio
async def test_committed_transaction_keeps_the_mark(
    mongo_client_replica: MongoClient,
) -> None:
    store = await _store(mongo_client_replica)

    async with mongo_client_replica.transaction():
        assert await store.mark_if_unseen("events", "m1") is True

    assert await store.mark_if_unseen("events", "m1") is False


@pytest.mark.asyncio
async def test_enlistment_tracks_transaction_scope(
    mongo_client_replica: MongoClient,
) -> None:
    store = await _store(mongo_client_replica)

    assert store.is_transactionally_enlisted() is False

    async with mongo_client_replica.transaction():
        # Force the (possibly lazy) transaction to materialize before asking.
        await store.mark_if_unseen("events", "m0")
        assert store.is_transactionally_enlisted() is True
