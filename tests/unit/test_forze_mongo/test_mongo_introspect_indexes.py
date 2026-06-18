"""Unit tests for Mongo index introspection."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from forze_mongo.kernel.introspect import MongoIntrospector
from forze_mongo.kernel.client import MongoClient


@pytest.mark.asyncio
async def test_list_indexes_parses_and_caches() -> None:
    client = MagicMock(spec=MongoClient)
    client.list_indexes = AsyncMock(
        return_value=[
            {"name": "_id_", "key": {"_id": 1}, "unique": False},
            {"name": "email_1", "key": {"email": 1}, "unique": True},
        ],
    )

    intro = MongoIntrospector(client=client)
    indexes = await intro.list_indexes(database="app", collection="docs")

    assert len(indexes) == 2
    assert indexes[1].name == "email_1"
    assert indexes[1].unique is True
    assert indexes[1].keys == (("email", 1),)

    again = await intro.list_indexes(database="app", collection="docs")
    assert again == indexes
    assert client.list_indexes.await_count == 1


@pytest.mark.asyncio
async def test_invalidate_collection_clears_cache() -> None:
    client = MagicMock(spec=MongoClient)
    client.list_indexes = AsyncMock(
        return_value=[{"name": "_id_", "key": {"_id": 1}}],
    )

    intro = MongoIntrospector(client=client)
    await intro.list_indexes(database="app", collection="docs")
    intro.invalidate_collection(database="app", collection="docs")
    await intro.list_indexes(database="app", collection="docs")

    assert client.list_indexes.await_count == 2

@pytest.mark.asyncio
async def test_list_indexes_preserves_special_index_directions() -> None:
    # Regression: non-btree indexes carry a STRING direction
    # ("text"/"2dsphere"/"hashed"/"vector"); int(v) used to crash here.
    client = MagicMock(spec=MongoClient)
    client.list_indexes = AsyncMock(
        return_value=[
            {"name": "title_text", "key": {"title": "text"}, "unique": False},
            {"name": "loc_2dsphere", "key": {"loc": "2dsphere"}, "unique": False},
            {"name": "h_hashed", "key": {"h": "hashed"}, "unique": False},
            {"name": "mixed", "key": {"a": 1, "b": -1}, "unique": False},
        ],
    )

    intro = MongoIntrospector(client=client)
    indexes = await intro.list_indexes(database="app", collection="docs")

    assert indexes[0].keys == (("title", "text"),)
    assert indexes[1].keys == (("loc", "2dsphere"),)
    assert indexes[2].keys == (("h", "hashed"),)
    assert indexes[3].keys == (("a", 1), ("b", -1))
