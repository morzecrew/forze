"""Integration tests: Pydantic computed fields are not persisted to MongoDB."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze_mongo.execution.deps import MongoDocumentConfig
from forze_mongo.execution.deps.deps import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient
from tests.support.execution_context import context_from_deps

from tests.integration._computed_field_models import (
    ComputedCreate,
    ComputedReadDoc,
    ComputedStoredDoc,
    ComputedUpdate,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_document_computed_field_roundtrip_not_persisted(
    mongo_client: MongoClient,
) -> None:
    collection = f"computed_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name

    spec = DocumentSpec(
        name="computed_docs",
        read=ComputedReadDoc,
        write={
            "domain": ComputedStoredDoc,
            "create_cmd": ComputedCreate,
            "update_cmd": ComputedUpdate,
        },
    )

    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db_name, collection),
            write=(db_name, collection),
        )
    )
    ctx = context_from_deps(Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            })
    )
    adapter = ctx.document.command(spec)

    created = await adapter.create(ComputedCreate(value=5))
    assert created.doubled == 10

    coll = await mongo_client.collection(collection, db_name=db_name)
    raw = await mongo_client.find_one(coll, {"_id": str(created.id)})
    assert raw is not None
    assert "doubled" not in raw

    fetched = await adapter.get(created.id)
    assert fetched.doubled == 10

    updated = await adapter.update(
        created.id,
        created.rev,
        ComputedUpdate(value=7),
    )
    assert updated.doubled == 14

    raw_after = await mongo_client.find_one(coll, {"_id": str(created.id)})
    assert raw_after is not None
    assert "doubled" not in raw_after
    assert raw_after["value"] == 7
