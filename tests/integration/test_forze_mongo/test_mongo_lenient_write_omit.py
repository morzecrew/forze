"""Integration tests: lenient read fields and write-omit fields on Mongo documents."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.execution.deps.utils import doc_write_gw, read_gw
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps


class OmitDomain(Document):
    name: str
    label: str = "n/a"  # not persisted to the collection


class OmitCreate(CreateDocumentCmd):
    name: str


class OmitUpdate(BaseDTO):
    name: str | None = None


def _write_types() -> DocumentWriteTypes[OmitDomain, OmitCreate, OmitUpdate]:
    return DocumentWriteTypes(domain=OmitDomain, create_cmd=OmitCreate, update_cmd=OmitUpdate)


@pytest.fixture
def ctx(mongo_client: MongoClient) -> ExecutionContext:
    return context_from_deps(Deps.plain({MongoClientDepKey: mongo_client}))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_write_omit_field_stripped_from_document(
    mongo_client: MongoClient, ctx: ExecutionContext
) -> None:
    db_name = (await mongo_client.db()).name
    collection = f"mongo_omit_{uuid4().hex[:8]}"
    relation = (db_name, collection)

    write = doc_write_gw(
        ctx,
        write_types=_write_types(),
        write_relation=relation,
        tenant_aware=False,
        write_omit_fields=frozenset({"label"}),
    )

    created = await write.create(OmitCreate(name="Ada"))
    assert created.label == "n/a"  # hydrated from the domain default on read-back

    # The field is genuinely absent from the stored document (not written as "n/a").
    coll = await mongo_client.collection(collection, db_name=db_name)
    raw = await mongo_client.find_one(coll, {"_id": str(created.id)})
    assert raw is not None
    assert "label" not in raw
    assert raw["name"] == "Ada"

    updated, _ = await write.update(created.id, OmitUpdate(name="Ada Lovelace"))
    assert updated.name == "Ada Lovelace"
    assert updated.label == "n/a"


class _ReadWithExtra(ReadDocument):
    name: str
    nickname: str = "anon"  # declared on the read model, not stored


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lenient_read_field_hydrates_from_default(
    mongo_client: MongoClient, ctx: ExecutionContext
) -> None:
    db_name = (await mongo_client.db()).name
    collection = f"mongo_lenient_{uuid4().hex[:8]}"
    relation = (db_name, collection)

    # Seed a complete document that simply has no ``nickname`` field.
    coll = await mongo_client.collection(collection, db_name=db_name)
    row_id = uuid4()
    now = datetime.now(UTC)
    await mongo_client.insert_one(
        coll,
        {
            "_id": str(row_id),
            "name": "Ada",
            "rev": 1,
            "created_at": now,
            "last_update_at": now,
        },
    )

    read = read_gw(
        ctx,
        read_type=_ReadWithExtra,
        read_relation=relation,
        tenant_aware=False,
        lenient_read_fields=frozenset({"nickname"}),
    )

    fetched = await read.get(row_id)
    assert fetched.name == "Ada"
    assert fetched.nickname == "anon"  # from the model default
    # The lenient field is not part of the gateway's stored-field bounds.
    assert "nickname" not in read.read_fields
