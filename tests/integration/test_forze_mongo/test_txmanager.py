"""Integration tests for MongoTxManagerAdapter.

Requires a replica set (mongo_client_replica) because MongoDB transactions
only work on replica set members.
"""

from uuid import uuid4

import pytest

from forze_mongo.adapters import MongoTxManagerAdapter, MongoTxScopeKey
from forze_mongo.kernel.platform import MongoClient


@pytest.fixture
def mongo_txmanager(mongo_client_replica: MongoClient) -> MongoTxManagerAdapter:
    """Provide a MongoTxManagerAdapter backed by the replica set Mongo client."""
    return MongoTxManagerAdapter(client=mongo_client_replica)


@pytest.mark.asyncio
async def test_scope_key(mongo_txmanager: MongoTxManagerAdapter) -> None:
    """scope_key returns MongoTxScopeKey."""
    assert mongo_txmanager.scope_key() == MongoTxScopeKey
    assert mongo_txmanager.scope_key().name == "mongo"


@pytest.mark.asyncio
async def test_transaction_commit(
    mongo_client_replica: MongoClient, mongo_txmanager: MongoTxManagerAdapter
) -> None:
    """Transaction commits when block exits normally."""
    coll_name = f"txmanager_commit_{uuid4().hex[:8]}"
    coll = mongo_client_replica.collection(coll_name)

    async with mongo_txmanager.transaction():
        await mongo_client_replica.insert_one(coll, {"value": 42})
        doc = await mongo_client_replica.find_one(coll, {"value": 42})
        assert doc is not None
        assert doc["value"] == 42

    doc_after = await mongo_client_replica.find_one(coll, {"value": 42})
    assert doc_after is not None
    assert doc_after["value"] == 42


@pytest.mark.asyncio
async def test_transaction_rollback(
    mongo_client_replica: MongoClient, mongo_txmanager: MongoTxManagerAdapter
) -> None:
    """Transaction rolls back when block raises."""
    coll_name = f"txmanager_rollback_{uuid4().hex[:8]}"
    coll = mongo_client_replica.collection(coll_name)

    try:
        async with mongo_txmanager.transaction():
            await mongo_client_replica.insert_one(coll, {"value": 99})
            raise ValueError("rollback me")
    except ValueError:
        pass

    doc_after = await mongo_client_replica.find_one(coll, {"value": 99})
    assert doc_after is None


@pytest.mark.asyncio
async def test_transaction_nested_reuses_session(
    mongo_client_replica: MongoClient, mongo_txmanager: MongoTxManagerAdapter
) -> None:
    """Nested transaction blocks reuse the same session; both levels commit together."""
    coll_name = f"txmanager_nested_{uuid4().hex[:8]}"
    coll = mongo_client_replica.collection(coll_name)

    async with mongo_txmanager.transaction():
        await mongo_client_replica.insert_one(coll, {"value": 1})
        async with mongo_txmanager.transaction():
            await mongo_client_replica.insert_one(coll, {"value": 2})
        docs = await mongo_client_replica.find_many(coll, {})
        assert len(docs) == 2

    docs_after = await mongo_client_replica.find_many(coll, {})
    assert len(docs_after) == 2
    assert {d["value"] for d in docs_after} == {1, 2}


@pytest.mark.asyncio
async def test_transaction_rollback_from_nested(
    mongo_client_replica: MongoClient, mongo_txmanager: MongoTxManagerAdapter
) -> None:
    """Exception in nested block aborts the whole transaction (MongoDB has no savepoints)."""
    coll_name = f"txmanager_nested_rollback_{uuid4().hex[:8]}"
    coll = mongo_client_replica.collection(coll_name)

    try:
        async with mongo_txmanager.transaction():
            await mongo_client_replica.insert_one(coll, {"value": 1})
            async with mongo_txmanager.transaction():
                await mongo_client_replica.insert_one(coll, {"value": 2})
                raise ValueError("rollback inner")
    except ValueError:
        pass

    docs_after = await mongo_client_replica.find_many(coll, {})
    assert len(docs_after) == 0
