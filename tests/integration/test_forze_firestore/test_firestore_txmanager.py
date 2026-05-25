"""Integration tests for FirestoreTxManagerAdapter."""

from uuid import uuid4

import pytest

from forze_firestore.adapters import FirestoreTxManagerAdapter, FirestoreTxScopeKey
from forze_firestore.kernel.platform import FirestoreClient


@pytest.fixture
def firestore_txmanager(firestore_client: FirestoreClient) -> FirestoreTxManagerAdapter:
    return FirestoreTxManagerAdapter(client=firestore_client)


@pytest.mark.asyncio
async def test_scope_key(firestore_txmanager: FirestoreTxManagerAdapter) -> None:
    assert firestore_txmanager.scope_key is FirestoreTxScopeKey
    assert firestore_txmanager.scope_key.name == "firestore"


@pytest.mark.asyncio
async def test_transaction_commit(
    firestore_client: FirestoreClient,
    firestore_txmanager: FirestoreTxManagerAdapter,
) -> None:
    coll_name = f"tx_commit_{uuid4().hex[:8]}"
    coll = await firestore_client.collection(coll_name)

    async with firestore_txmanager.transaction():
        await firestore_client.set_document(coll, "doc-1", {"value": 42})

    doc_after = await firestore_client.get_document(coll, "doc-1")
    assert doc_after is not None
    assert doc_after["value"] == 42


@pytest.mark.asyncio
async def test_transaction_rollback(
    firestore_client: FirestoreClient,
    firestore_txmanager: FirestoreTxManagerAdapter,
) -> None:
    coll_name = f"tx_rollback_{uuid4().hex[:8]}"
    coll = await firestore_client.collection(coll_name)

    try:
        async with firestore_txmanager.transaction():
            await firestore_client.set_document(coll, "doc-1", {"value": 99})
            raise ValueError("rollback me")
    except ValueError:
        pass

    doc_after = await firestore_client.get_document(coll, "doc-1")
    assert doc_after is None


@pytest.mark.asyncio
async def test_transaction_nested_reuses_transaction(
    firestore_client: FirestoreClient,
    firestore_txmanager: FirestoreTxManagerAdapter,
) -> None:
    coll_name = f"tx_nested_{uuid4().hex[:8]}"
    coll = await firestore_client.collection(coll_name)

    async with firestore_txmanager.transaction():
        await firestore_client.set_document(coll, "doc-1", {"value": 1})
        async with firestore_txmanager.transaction():
            await firestore_client.set_document(coll, "doc-2", {"value": 2})

    rows = await firestore_client.query_stream(coll, limit=10)
    assert len(rows) == 2
