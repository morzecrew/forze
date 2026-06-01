"""Integration tests for :meth:`~forze_mongo.kernel.client.MongoClient.find_one_and_update`."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze_mongo.kernel.client import MongoClient


@pytest.mark.asyncio
async def test_find_one_and_update_returns_modified_document(
    mongo_client: MongoClient,
) -> None:
    coll_name = f"fou_{uuid4().hex[:8]}"
    coll = await mongo_client.collection(coll_name)

    await mongo_client.insert_one(coll, {"name": "alpha", "status": "pending"})
    doc = await mongo_client.find_one_and_update(
        coll,
        {"status": "pending"},
        {"$set": {"status": "done"}},
        sort=[("name", 1)],
    )

    assert doc is not None
    assert doc["status"] == "done"
    assert doc["name"] == "alpha"

    remaining = await mongo_client.find_one(coll, {"status": "pending"})
    assert remaining is None
