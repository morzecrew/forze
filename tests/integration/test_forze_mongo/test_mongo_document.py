from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document.specs import DocumentSpec
from forze.application.contracts.query import QueryFilterExpression
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import ConflictError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps.deps import mongo_document_configurable
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient


class MyDoc(Document):
    name: str
    is_deleted: bool = False


class MyCreateDoc(CreateDocumentCmd):
    name: str


class MyUpdateDoc(BaseDTO):
    name: str | None = None


class MyReadDoc(ReadDocument):
    name: str
    is_deleted: bool = False


@pytest.mark.asyncio
async def test_mongo_document_adapter_roundtrip(mongo_client: MongoClient) -> None:
    collection = f"docs_{uuid4().hex[:8]}"
    history_collection = f"{collection}_history"
    deps = Deps({MongoClientDepKey: mongo_client})
    ctx = ExecutionContext(deps=deps)
    spec = DocumentSpec(
        namespace="my_docs_ns",
        read={"source": collection, "model": MyReadDoc},
        write={
            "source": collection,
            "models": {
                "domain": MyDoc,
                "create_cmd": MyCreateDoc,
                "update_cmd": MyUpdateDoc,
            },
        },
        history={"source": history_collection},
    )

    factory = mongo_document_configurable(
        rev_bump_strategy="application",
        history_write_strategy="application",
    )
    adapter = factory(ctx, spec)

    created = await adapter.create(MyCreateDoc(name="alpha"))
    created_2 = await adapter.create(MyCreateDoc(name="beta"))
    assert isinstance(created.id, UUID)
    assert created.rev == 1

    fetched = await adapter.get(created.id)
    assert fetched.name == "alpha"

    filtered: QueryFilterExpression = {"$fields": {"name": {"$eq": "alpha"}}}
    found = await adapter.find(filtered)
    assert found is not None
    assert found.id == created.id

    docs, total = await adapter.find_many(limit=10)
    assert total == 2
    assert {x.id for x in docs} == {created.id, created_2.id}

    updated = await adapter.update(created.id, MyUpdateDoc(name="alpha-2"), rev=created.rev)
    assert updated.name == "alpha-2"
    assert updated.rev == 2

    with pytest.raises(ConflictError, match="Historical consistency violation"):
        await adapter.update(created.id, MyUpdateDoc(name="alpha-3"), rev=1)

    touched = await adapter.touch(created.id)
    assert touched.rev == 3

    deleted = await adapter.delete(created.id)
    assert deleted.is_deleted is True

    restored = await adapter.restore(created.id)
    assert restored.is_deleted is False

    await adapter.kill(created_2.id)
    assert await adapter.count() == 1

    history_rows = await mongo_client.find_many(
        mongo_client.collection(history_collection),
        {"source": collection, "id": str(created.id)},
    )
    assert len(history_rows) >= 3

    await adapter.kill(created.id)
    assert await adapter.count() == 0
