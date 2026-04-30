from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.contracts.query import QueryFilterExpression
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import ConflictError
from forze.domain.mixins import SoftDeletionMixin
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps.deps import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.platform import MongoClient


class MyDoc(Document, SoftDeletionMixin):
    name: str


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
    db_name = (await mongo_client.db()).name

    spec = DocumentSpec(
        name="my_docs_ns",
        read=MyReadDoc,
        write={
            "domain": MyDoc,
            "create_cmd": MyCreateDoc,
            "update_cmd": MyUpdateDoc,
        },
        history_enabled=True,
    )

    configurable = ConfigurableMongoDocument(
        config={
            "read": (db_name, collection),
            "write": (db_name, collection),
            "history": (db_name, history_collection),
        }
    )
    deps = Deps.plain(
        {
            MongoClientDepKey: mongo_client,
            DocumentQueryDepKey: configurable,
            DocumentCommandDepKey: configurable,
        }
    )
    ctx = ExecutionContext(deps=deps)
    adapter = ctx.doc_command(spec)

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

    __p = await adapter.find_many(
        pagination={"limit": 10},
        return_count=True,
    )
    docs = __p.hits
    total = __p.count
    assert total == 2
    assert {x.id for x in docs} == {created.id, created_2.id}

    updated = await adapter.update(
        created.id,
        created.rev,
        MyUpdateDoc(name="alpha-2"),
    )
    assert updated.name == "alpha-2"
    assert updated.rev == 2

    with pytest.raises(ConflictError, match="Historical consistency violation"):
        await adapter.update(created.id, 1, MyUpdateDoc(name="alpha-3"))

    touched = await adapter.touch(created.id)
    assert touched.rev == 3

    deleted = await adapter.delete(created.id, touched.rev)
    assert deleted.is_deleted is True

    restored = await adapter.restore(created.id, deleted.rev)
    assert restored.is_deleted is False

    await adapter.kill(created_2.id)
    assert await adapter.count() == 1

    history_rows = await mongo_client.find_many(
        await mongo_client.collection(history_collection),
        {"source": f"{db_name}.{collection}", "id": str(created.id)},
    )
    assert len(history_rows) >= 3

    await adapter.kill(created.id)
    assert await adapter.count() == 0


@pytest.mark.asyncio
async def test_mongo_document_find_many_sorted(mongo_client: MongoClient) -> None:
    """find_many honours sort order on the read model field."""
    collection = f"docs_sort_{uuid4().hex[:8]}"
    history_collection = f"{collection}_history"
    db_name = (await mongo_client.db()).name

    spec = DocumentSpec(
        name="mongo_sort_ns",
        read=MyReadDoc,
        write={
            "domain": MyDoc,
            "create_cmd": MyCreateDoc,
            "update_cmd": MyUpdateDoc,
        },
        history_enabled=True,
    )

    configurable = ConfigurableMongoDocument(
        config={
            "read": (db_name, collection),
            "write": (db_name, collection),
            "history": (db_name, history_collection),
        }
    )
    deps = Deps.plain(
        {
            MongoClientDepKey: mongo_client,
            DocumentQueryDepKey: configurable,
            DocumentCommandDepKey: configurable,
        }
    )
    ctx = ExecutionContext(deps=deps)
    adapter = ctx.doc_command(spec)

    for name in ("charlie", "alice", "bob"):
        await adapter.create(MyCreateDoc(name=name))

    q_adapter = ctx.doc_query(spec)

    __p = await q_adapter.find_many(
        None,
        pagination={"limit": 10, "offset": 0},
        sorts={"name": "asc"},
        return_count=True,
    )
    rows = __p.hits
    total = __p.count

    assert total == 3
    assert [r.name for r in rows] == ["alice", "bob", "charlie"]

    rows_desc = await q_adapter.find_many(
        None,
        pagination={"limit": 2, "offset": 0},
        sorts={"name": "desc"},
    )
    assert [r.name for r in rows_desc.hits] == ["charlie", "bob"]
