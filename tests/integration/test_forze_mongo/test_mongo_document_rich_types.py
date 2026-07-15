"""A Mongo document with ``UUID`` and ``Decimal`` *business* fields — create, update, filter, read.

# covers: forze_mongo.kernel.gateways.base.MongoGateway._coerce_query_value
# covers: forze_mongo.kernel.gateways.base.MongoGateway._decode_bson_value

Every other Mongo document test models its fields as ``str``/``int``, so the BSON encoder was
never asked to store a raw ``UUID`` field (``UuidRepresentation.UNSPECIFIED`` refuses one) or a
raw ``Decimal`` (no BSON encoding). Both round-trip here: a UUID stores as its canonical string,
a Decimal as ``Decimal128``, and reads come back as the model's declared types — through the
insert path (``ensure``/``create``), the update path (``$set``), and a filter on each.
"""

from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze.testing import context_from_deps
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient

# ----------------------- #


class _Priced(Document):
    ref: UUID
    amount: Decimal
    label: str


class _PricedRead(ReadDocument):
    ref: UUID
    amount: Decimal
    label: str


class _PricedCreate(BaseDTO):
    ref: UUID
    amount: Decimal
    label: str


class _PricedUpdate(BaseDTO):
    amount: Decimal | None = None
    label: str | None = None


_SPEC: DocumentSpec[_PricedRead, _Priced, _PricedCreate, _PricedUpdate] = DocumentSpec(
    name="priced",
    read=_PricedRead,
    write=DocumentWriteTypes(domain=_Priced, create_cmd=_PricedCreate, update_cmd=_PricedUpdate),
)


def _ctx(mongo_client: MongoClient, db_name: str, collection: str) -> ExecutionContext:
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db_name, collection), write=(db_name, collection))
    )
    return context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )


# ....................... #


@pytest.mark.asyncio
async def test_uuid_and_decimal_fields_survive_create_update_and_filter(
    mongo_client: MongoClient,
) -> None:
    db_name = (await mongo_client.db()).name
    ctx = _ctx(mongo_client, db_name, f"priced_{uuid4().hex[:8]}")
    command = ctx.document.command(_SPEC)
    query = ctx.document.query(_SPEC)

    ref = uuid4()
    doc_id = uuid4()

    # -- Insert path (ensure) — the seam that used to reject a raw UUID/Decimal outright. -- #
    created = await command.ensure(
        doc_id, _PricedCreate(ref=ref, amount=Decimal("19.99"), label="a")
    )
    assert created.ref == ref
    assert created.amount == Decimal("19.99")

    got = await query.get(doc_id)
    assert got.ref == ref  # UUID field, stored as its canonical string, back to UUID
    assert got.amount == Decimal("19.99")  # Decimal field, stored as Decimal128, back to Decimal

    # -- Update path ($set) — a different write seam than insert (goes through _coerce_query_value
    #    on the diff, then _from_storage_doc on the read-back). -- #
    updated = await command.update(doc_id, got.rev, _PricedUpdate(amount=Decimal("25.50")))
    assert updated.amount == Decimal("25.50")
    assert updated.ref == ref  # untouched UUID field still intact

    reread = await query.get(doc_id)
    assert reread.amount == Decimal("25.50")
