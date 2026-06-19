"""Integration tests for ``lazy_transaction`` on Firestore (emulator).

With ``lazy_transaction`` (the default) a transaction scope issues no ``_begin``
until the first operation, so a scope that runs no operation does no server round
trip. Once materialized the scope commits on a clean exit and rolls back on
error — even when the first operation runs in a different context than the scope
opener (as the resilience executor does).
"""

import asyncio

import pytest

pytest.importorskip("google.cloud.firestore")

from forze_firestore.kernel.client import FirestoreClient

# ----------------------- #


def _bound_tx(client: FirestoreClient):
    """The effective transaction for the current context (white-box), or ``None``."""

    return client._FirestoreClient__current_transaction()  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_lazy_scope_defers_begin_until_first_op(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    client = firestore_client
    coll = await client.collection(unique_collection)

    async with client.transaction():
        # Logically in a transaction, but nothing begun / bound yet.
        assert client.is_in_transaction() is True
        assert _bound_tx(client) is None

        await client.set_document(coll, "doc-1", {"value": 1})

        # The first operation materialized the transaction.
        assert _bound_tx(client) is not None

    assert _bound_tx(client) is None
    doc = await client.get_document(coll, "doc-1")
    assert doc is not None and doc["value"] == 1


@pytest.mark.asyncio
async def test_empty_lazy_scope_is_a_noop(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    client = firestore_client
    coll = await client.collection(unique_collection)

    async with client.transaction():
        pass  # never materialized

    # The client is immediately usable for subsequent work.
    await client.set_document(coll, "doc-1", {"value": 1})
    assert (await client.get_document(coll, "doc-1"))["value"] == 1


@pytest.mark.asyncio
async def test_first_op_in_child_context_commits(
    firestore_client: FirestoreClient,
    unique_collection: str,
) -> None:
    """The first operation may materialize the scope in a child context (as the
    resilience executor runs it); the scope must still commit."""

    client = firestore_client
    coll = await client.collection(unique_collection)

    async with client.transaction():
        await asyncio.create_task(client.set_document(coll, "doc-1", {"value": 1}))
        assert _bound_tx(client) is not None

    doc = await client.get_document(coll, "doc-1")
    assert doc is not None and doc["value"] == 1
