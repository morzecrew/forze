"""Integration tests for :class:`~forze_mongo.kernel.gateways.read.MongoReadGateway` and write gateway against MongoDB."""

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentWriteTypes
from forze.application.execution import Deps, ExecutionContext
from forze.base.errors import InfrastructureError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document
from forze_mongo.adapters import MongoTxManagerAdapter
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.execution.deps.utils import doc_write_gw
from forze_mongo.kernel.platform import MongoClient


class GwDoc(Document):
    name: str


class GwCreate(CreateDocumentCmd):
    name: str


class GwUpdate(BaseDTO):
    name: str | None = None


def _gw_write_types() -> DocumentWriteTypes[GwDoc, GwCreate, GwUpdate]:
    return DocumentWriteTypes(
        domain=GwDoc,
        create_cmd=GwCreate,
        update_cmd=GwUpdate,
    )


@pytest.fixture
def mongo_gw_ctx(mongo_client: MongoClient) -> ExecutionContext:
    deps = Deps.plain({MongoClientDepKey: mongo_client})
    return ExecutionContext(deps=deps)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_gateways_create_read_projections_and_list(
    mongo_client: MongoClient,
    mongo_gw_ctx: ExecutionContext,
) -> None:
    """Exercise read/write gateways: create, projections, find, bounded list, count."""
    db_name = mongo_client.db().name
    collection = f"mongo_gw_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    ctx = mongo_gw_ctx

    write = doc_write_gw(
        ctx,
        write_types=_gw_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    read = write.read_gw

    created = await write.create(GwCreate(name="gateway-one"))
    other = await write.create(GwCreate(name="gateway-two"))

    full = await read.get(created.id)
    assert full.name == "gateway-one"

    proj = await read.get(created.id, return_fields=["name"])
    assert proj["name"] == "gateway-one"

    many_proj = await read.get_many(
        [created.id, other.id],
        return_fields=["id", "name"],
    )
    assert {row["name"] for row in many_proj} == {"gateway-one", "gateway-two"}

    one = await read.find(
        {"$fields": {"name": {"$eq": "gateway-one"}}},
        return_fields=["name"],
    )
    assert one is not None
    assert one["name"] == "gateway-one"

    listed = await read.find_many(limit=10)
    assert len(listed) >= 2

    total = await read.count(None)
    assert total >= 2

    updated, _ = await write.update(created.id, GwUpdate(name="patched"))
    assert updated.name == "patched"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_read_gateway_for_update_requires_transaction(
    mongo_client_replica: MongoClient,
) -> None:
    """``for_update=True`` uses :meth:`MongoClient.require_transaction` (replica set)."""
    db_name = mongo_client_replica.db().name
    collection = f"mongo_gw_tx_{uuid4().hex[:8]}"
    relation = (db_name, collection)
    ctx = ExecutionContext(deps=Deps.plain({MongoClientDepKey: mongo_client_replica}))

    write = doc_write_gw(
        ctx,
        write_types=_gw_write_types(),
        write_relation=relation,
        history_enabled=False,
        tenant_aware=False,
    )
    read = write.read_gw

    created = await write.create(GwCreate(name="tx-doc"))

    with pytest.raises(InfrastructureError, match="Transactional context is required"):
        await read.get(created.id, for_update=True)

    tx = MongoTxManagerAdapter(client=mongo_client_replica)
    async with tx.transaction():
        locked = await read.get(created.id, for_update=True)
        assert locked.name == "tx-doc"
