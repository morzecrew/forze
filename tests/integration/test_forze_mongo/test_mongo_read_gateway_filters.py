"""Mongo read gateway filter quantifiers and projection-only finds."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.execution import Deps, ExecutionContext
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.execution.deps.utils import doc_write_gw
from forze_mongo.kernel.platform import MongoClient
from tests.support import (
    IntegrationCreateCmd,
    IntegrationDocument,
    IntegrationUpdateCmd,
    IsPartialDict,
    IsUUID,
    make_create_cmd,
)


class MongoGwDoc(IntegrationDocument):
    category: str = "default"


class MongoGwCreate(IntegrationCreateCmd):
    name: str
    category: str = "default"


@pytest.fixture
def mongo_filter_ctx(mongo_client: MongoClient) -> ExecutionContext:
    return ExecutionContext(deps=Deps.plain({MongoClientDepKey: mongo_client}))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_read_gateway_and_filter_with_projection(
    mongo_client: MongoClient,
    mongo_filter_ctx: ExecutionContext,
) -> None:
    db_name = (await mongo_client.db()).name
    collection = f"mongo_filt_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    write = doc_write_gw(
        mongo_filter_ctx,
        write_types={
            "domain": MongoGwDoc,
            "create_cmd": MongoGwCreate,
            "update_cmd": IntegrationUpdateCmd,
        },
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    read = write.read_gw

    await write.create(MongoGwCreate(name="a", category="books"))
    await write.create(MongoGwCreate(name="b", category="hardware"))

    row = await read.find(
        {
            "$and": [
                {"$values": {"category": "books"}},
                {"$values": {"name": "a"}},
            ],
        },
        return_fields=["id", "name", "category"],
    )
    assert row is not None
    assert row == IsPartialDict(
        {"name": "a", "category": "books", "id": IsUUID},
    )

    empty_and = await read.find_many({"$and": []}, limit=10, offset=0)
    assert len(empty_and) >= 2

    proj = await read.find(
        {"$values": {"name": "b"}},
        return_fields=["name"],
    )
    assert proj is not None
    assert set(proj.keys()) == {"name"}
