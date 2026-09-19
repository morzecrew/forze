"""Integration: correction lineage on a real Mongo, including the exemption it needs.

A versioned aggregate declares `UniqueTogether(("supersedes_id",), skip_null=True)` — one
successor per predecessor, exempting the first versions that supersede nothing — and the whole
question is whether Mongo can express that exemption. It can, but not the obvious way: `sparse`
still indexes an *explicit* null, so two first versions collide under it. A
`partialFilterExpression` naming what the field **is** excludes nulls exactly, because a partial
filter admits no negation and a type predicate is the way round it.

Every leg here runs against the server rather than a fixture, because the claim is about what
Mongo does, and the sparse legs are the contrast that makes the `$type` ones mean something.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid4

import pytest
from pymongo.errors import DuplicateKeyError

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import ReadDocument
from forze_kits.aggregates.versioned import (
    ONE_CURRENT_VERSION,
    ONE_SUCCESSOR,
    CorrectDocument,
    CorrectDocumentDTO,
)
from forze_kits.domain.versioned import (
    CorrectionDoc,
    CreateCmdWithVersioningFields,
    CreateCorrectionCmd,
    DocWithVersioning,
    UpdateCmdWithVersioning,
)
from forze_mongo.execution.deps import MongoDepsModule
from forze_mongo.execution.deps.configs import MongoDocumentConfig
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.introspect import MongoIntrospector
from forze_mongo.kernel.validate_indexes import (
    MongoDocumentIndexSpec,
    validate_mongo_document_indexes,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


class Reading(DocWithVersioning):
    meter: str
    kwh: int = 0


class ReadingCreate(CreateCmdWithVersioningFields):
    meter: str
    kwh: int = 0


class ReadingUpdate(UpdateCmdWithVersioning):
    meter: str | None = None
    kwh: int | None = None


class ReadingRead(ReadDocument):
    meter: str
    kwh: int = 0
    root_id: UUID
    version: int = 1
    supersedes_id: UUID | None = None
    is_current: bool = True
    superseded_at: datetime | None = None


class CorrectionRead(ReadDocument):
    root_id: UUID
    from_id: UUID
    to_id: UUID
    actor_id: UUID | None = None
    reason: str


def _spec(name: str) -> DocumentSpec[ReadingRead, Reading, ReadingCreate, ReadingUpdate]:
    return DocumentSpec[ReadingRead, Reading, ReadingCreate, ReadingUpdate](
        name=name,
        read=ReadingRead,
        write=DocumentWriteTypes(
            domain=Reading, create_cmd=ReadingCreate, update_cmd=ReadingUpdate
        ),
        guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
    )


async def _collections(mongo_client: MongoClient) -> tuple[str, str, str]:
    """A readings collection with both guarantees migrated, and a corrections collection."""

    db_name = (await mongo_client.db()).name
    readings = f"readings_{uuid4().hex[:8]}"
    corrections = f"corrections_{uuid4().hex[:8]}"

    coll = await mongo_client.collection(readings, db_name=db_name)
    await coll.create_index(
        [("root_id", 1)], unique=True, partialFilterExpression={"is_current": True}
    )
    # The exemption: index a row only when its pointer *is* a string, which is what this adapter
    # writes a UUID as — so the null pointers every first version carries are left out.
    await coll.create_index(
        [("supersedes_id", 1)],
        unique=True,
        partialFilterExpression={"supersedes_id": {"$type": "string"}},
    )

    return db_name, readings, corrections


def _ctx(mongo_client: MongoClient, db: str, readings: str, corrections: str):
    return context_from_deps(
        MongoDepsModule(
            client=mongo_client,
            rw_documents={
                readings: MongoDocumentConfig(read=(db, readings), write=(db, readings)),
                corrections: MongoDocumentConfig(
                    read=(db, corrections), write=(db, corrections)
                ),
            },
        )()
    )


# ....................... #


class TestTheExemptionMongoActuallyHas:
    """`sparse` is not the mechanism; a type predicate is. Both proved against the server."""

    async def test_a_sparse_index_collides_on_two_null_pointers(
        self, mongo_client: MongoClient
    ) -> None:
        db_name, readings, _ = await _collections(mongo_client)
        coll = await mongo_client.collection(f"{readings}_sparse", db_name=db_name)
        await coll.create_index([("supersedes_id", 1)], unique=True, sparse=True)

        await coll.insert_one({"_id": "a", "supersedes_id": None})

        # Two first versions, each with an explicit null pointer — which sparse indexes.
        with pytest.raises(DuplicateKeyError):
            await coll.insert_one({"_id": "b", "supersedes_id": None})

    async def test_the_type_predicate_lets_them_through(
        self, mongo_client: MongoClient
    ) -> None:
        db_name, readings, _ = await _collections(mongo_client)
        coll = await mongo_client.collection(readings, db_name=db_name)

        await coll.insert_one({"_id": "a", "supersedes_id": None})
        await coll.insert_one({"_id": "b", "supersedes_id": None})

        rows = await coll.find({}).to_list(None)

        assert len(rows) == 2

    async def test_and_still_refuses_two_successors_of_one_predecessor(
        self, mongo_client: MongoClient
    ) -> None:
        # The contrast: exempting nulls must not exempt everything.
        db_name, readings, _ = await _collections(mongo_client)
        coll = await mongo_client.collection(readings, db_name=db_name)
        pointer = str(uuid4())

        await coll.insert_one({"_id": "a", "supersedes_id": pointer})

        with pytest.raises(DuplicateKeyError):
            await coll.insert_one({"_id": "b", "supersedes_id": pointer})


# ....................... #


class TestACorrectionAgainstMongo:
    async def test_it_supersedes_under_the_real_indexes(
        self, mongo_client: MongoClient
    ) -> None:
        db_name, readings, corrections = await _collections(mongo_client)
        ctx = _ctx(mongo_client, db_name, readings, corrections)
        spec = _spec(readings)
        cmd = ctx.doc.command(spec)

        fact = uuid4()
        first = await cmd.create(
            ReadingCreate(meter="m-1", kwh=100, root_id=fact, version=1), id=fact
        )

        second = await CorrectDocument(
            doc=cmd,
            query=ctx.doc.query(spec),
            corrections=ctx.doc.command(
                DocumentSpec(
                    name=corrections,
                    read=CorrectionRead,
                    write=DocumentWriteTypes(
                        domain=CorrectionDoc, create_cmd=CreateCorrectionCmd
                    ),
                )
            ),
            create_cmd=ReadingCreate,
            actor=ctx.inv_ctx.get_authn,
        )(
            CorrectDocumentDTO(
                id=first.id,
                expected_version=1,
                dto=ReadingUpdate(kwh=120),
                reason="meter misread",
            )
        )

        assert second.version == 2
        assert second.kwh == 120
        assert second.meter == "m-1"

        coll = await mongo_client.collection(readings, db_name=db_name)
        rows = await coll.find({}, sort=[("version", 1)]).to_list(None)

        assert [row["is_current"] for row in rows] == [False, True]

    async def test_two_first_versions_coexist(self, mongo_client: MongoClient) -> None:
        # The case the exemption exists for, through the adapter rather than the driver: every
        # first version carries a null pointer, so a guarantee counting nulls as values would
        # refuse the second fact ever created.
        db_name, readings, corrections = await _collections(mongo_client)
        ctx = _ctx(mongo_client, db_name, readings, corrections)
        cmd = ctx.doc.command(_spec(readings))

        for _ in range(3):
            fact = uuid4()
            await cmd.create(
                ReadingCreate(meter="m", kwh=1, root_id=fact, version=1), id=fact
            )

        coll = await mongo_client.collection(readings, db_name=db_name)

        assert await coll.count_documents({}) == 3


# ....................... #


class TestStartupValidation:
    @staticmethod
    async def _validate(mongo_client: MongoClient, db: str, readings: str) -> None:
        await validate_mongo_document_indexes(
            MongoIntrospector(client=mongo_client),
            [
                MongoDocumentIndexSpec(
                    name="readings",
                    write_relation=(db, readings),
                    guarantees=(ONE_CURRENT_VERSION, ONE_SUCCESSOR),
                    read_model=ReadingRead,
                )
            ],
        )

    async def test_both_indexes_present_passes(self, mongo_client: MongoClient) -> None:
        db_name, readings, _ = await _collections(mongo_client)

        await self._validate(mongo_client, db_name, readings)

    async def test_a_sparse_index_does_not_satisfy_the_exemption(
        self, mongo_client: MongoClient
    ) -> None:
        # The heart of it: sparse is what an operator reaches for, and it does not keep the
        # guarantee — so startup has to refuse it rather than accept the near-miss.
        db_name = (await mongo_client.db()).name
        readings = f"readings_{uuid4().hex[:8]}"
        coll = await mongo_client.collection(readings, db_name=db_name)
        await coll.create_index(
            [("root_id", 1)], unique=True, partialFilterExpression={"is_current": True}
        )
        await coll.create_index([("supersedes_id", 1)], unique=True, sparse=True)

        with pytest.raises(CoreException) as caught:
            await self._validate(mongo_client, db_name, readings)

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert "supersedes_id" in caught.value.summary

    async def test_the_printed_migration_creates_an_index_that_validates(
        self, mongo_client: MongoClient
    ) -> None:
        # The refusal has to be runnable, and for the exemption that means printing the type
        # predicate rather than a placeholder.
        db_name = (await mongo_client.db()).name
        readings = f"readings_{uuid4().hex[:8]}"
        coll = await mongo_client.collection(readings, db_name=db_name)
        await coll.create_index(
            [("root_id", 1)], unique=True, partialFilterExpression={"is_current": True}
        )

        with pytest.raises(CoreException) as caught:
            await self._validate(mongo_client, db_name, readings)

        statement = " ".join(caught.value.summary.split())

        assert 'partialFilterExpression: {supersedes_id: {$type: "string"}}' in statement

        await coll.create_index(
            [("supersedes_id", 1)],
            unique=True,
            partialFilterExpression={"supersedes_id": {"$type": "string"}},
        )

        await self._validate(mongo_client, db_name, readings)
