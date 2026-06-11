"""Integration tests for FirestoreClient transaction semantics (emulator).

Covers rollback on task cancellation, commit/rollback at the client level,
and count aggregation attached to the ambient context transaction.
"""

import asyncio
from uuid import uuid4

import pytest

pytest.importorskip("google.cloud.firestore")

from forze_firestore.kernel.client import FirestoreClient

# ----------------------- #


@pytest.mark.asyncio
async def test_transaction_commit_persists(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    coll = await firestore_client.collection(unique_collection)

    async with firestore_client.transaction():
        await firestore_client.set_document(coll, "doc-1", {"value": 1})

    doc = await firestore_client.get_document(coll, "doc-1")
    assert doc is not None
    assert doc["value"] == 1


@pytest.mark.asyncio
async def test_transaction_rollback_discards(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    coll = await firestore_client.collection(unique_collection)

    with pytest.raises(ValueError):
        async with firestore_client.transaction():
            await firestore_client.set_document(coll, "doc-1", {"value": 1})
            raise ValueError("rollback me")

    assert await firestore_client.get_document(coll, "doc-1") is None


@pytest.mark.asyncio
async def test_cancelled_transaction_rolls_back(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """Cancelling a task mid-transaction must roll back, not leak the tx."""

    coll = await firestore_client.collection(unique_collection)
    in_tx = asyncio.Event()

    async def work() -> None:
        async with firestore_client.transaction():
            # Read with the tx so the server-side transaction is exercised,
            # then buffer a write that must never commit.
            await firestore_client.get_document(coll, "doc-1")
            await firestore_client.set_document(coll, "doc-1", {"value": 1})
            in_tx.set()
            await asyncio.sleep(30)

    task = asyncio.create_task(work())
    await asyncio.wait_for(in_tx.wait(), timeout=10)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert await firestore_client.get_document(coll, "doc-1") is None

    # The client must be reusable afterwards (no dangling context state).
    assert not firestore_client.is_in_transaction()

    async with firestore_client.transaction():
        await firestore_client.set_document(coll, "doc-2", {"value": 2})

    doc = await firestore_client.get_document(coll, "doc-2")
    assert doc is not None


@pytest.mark.asyncio
async def test_count_documents_inside_transaction(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """Count runs against the ambient transaction (reads precede writes)."""

    coll = await firestore_client.collection(unique_collection)
    await firestore_client.set_document(coll, "a", {"v": 1})
    await firestore_client.set_document(coll, "b", {"v": 2})

    async with firestore_client.transaction():
        count_in_tx = await firestore_client.count_documents(coll)
        assert count_in_tx == 2

        await firestore_client.set_document(coll, "c", {"v": 3})

    assert await firestore_client.count_documents(coll) == 3


@pytest.mark.asyncio
async def test_collection_rejects_other_database(
    firestore_client: FirestoreClient,
) -> None:
    from forze.base.exceptions import CoreException, ExceptionKind

    with pytest.raises(CoreException) as ei:
        await firestore_client.collection(
            f"forze_{uuid4().hex[:8]}",
            database="not-the-configured-one",
        )

    assert ei.value.kind == ExceptionKind.CONFIGURATION
