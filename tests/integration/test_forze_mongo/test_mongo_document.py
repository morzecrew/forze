from uuid import UUID, uuid4

import pytest

from forze.application.contracts.query import QueryFilterExpression
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.adapters.document import MongoDocumentAdapter
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
    adapter = MongoDocumentAdapter(
        client=mongo_client,
        read_model=MyReadDoc,
        domain_model=MyDoc,
        create_dto=MyCreateDoc,
        update_dto=MyUpdateDoc,
        read_source=collection,
    )

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

    touched = await adapter.touch(created.id)
    assert touched.rev == 3

    deleted = await adapter.delete(created.id)
    assert deleted.is_deleted is True

    restored = await adapter.restore(created.id)
    assert restored.is_deleted is False

    await adapter.kill(created_2.id)
    assert await adapter.count() == 1

    await adapter.kill(created.id)
    assert await adapter.count() == 0
