"""Integration: a declared storage guarantee validated against a real Mongo.

The Mongo half of the same rule Postgres follows — reconciliation at wiring says the store
*can* keep a uniqueness guarantee, and only the live collection says whether the deployment
created the index. A filtered guarantee needs a filtered index, and the distinction is the one
worth testing: a plain unique index accepted for "one *current* document per fact" would refuse
every superseded document the guarantee meant to allow.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.guarantees import UniqueTogether
from forze.application.execution import Deps, LifecyclePlan
from forze.base.exceptions import CoreException
from forze_mongo.execution.deps import MongoClientDepKey
from forze_mongo.execution.document_indexes import (
    mongo_document_index_validation_lifecycle_step,
)
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.validate_indexes import MongoDocumentIndexSpec
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

ONE_CURRENT = UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
ONE_EVER = UniqueTogether(fields=("root_id",))


async def _collection(mongo_client: MongoClient) -> tuple[str, str]:
    db_name = (await mongo_client.db()).name
    collection = f"guarantee_{uuid4().hex[:8]}"
    coll = await mongo_client.collection(collection, db_name=db_name)
    await coll.insert_one({"_id": "seed", "root_id": "r0", "is_current": True})

    return db_name, collection


async def _validate(
    mongo_client: MongoClient,
    relation: tuple[str, str],
    *guarantees: UniqueTogether,
) -> None:
    step = mongo_document_index_validation_lifecycle_step(
        specs=[
            MongoDocumentIndexSpec(
                name="fact",
                write_relation=relation,
                guarantees=guarantees,
            ),
        ],
    )
    ctx = context_from_deps(Deps.plain({MongoClientDepKey: mongo_client}))

    await LifecyclePlan.from_steps(step).freeze().startup(ctx)


# ....................... #


class TestMongoStartupValidation:
    async def test_a_missing_index_refuses_and_names_the_one_that_would_serve(
        self,
        mongo_client: MongoClient,
    ) -> None:
        relation = await _collection(mongo_client)

        with pytest.raises(CoreException) as caught:
            await _validate(mongo_client, relation, ONE_CURRENT)

        message = caught.value.summary

        assert "createIndex" in message
        assert "root_id" in message
        assert "partialFilterExpression" in message
        assert "The migration is what satisfies a guarantee" in message

    async def test_a_partial_index_satisfies_a_filtered_guarantee(
        self,
        mongo_client: MongoClient,
    ) -> None:
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index(
            [("root_id", 1)],
            unique=True,
            partialFilterExpression={"is_current": True},
        )

        await _validate(mongo_client, (db_name, collection), ONE_CURRENT)

    async def test_a_plain_index_does_not_satisfy_a_filtered_guarantee(
        self,
        mongo_client: MongoClient,
    ) -> None:
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index([("root_id", 1)], unique=True)

        with pytest.raises(CoreException, match="no partial unique index"):
            await _validate(mongo_client, (db_name, collection), ONE_CURRENT)

    async def test_a_plain_index_satisfies_an_unfiltered_guarantee(
        self,
        mongo_client: MongoClient,
    ) -> None:
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index([("root_id", 1)], unique=True)

        await _validate(mongo_client, (db_name, collection), ONE_EVER)

    async def test_a_sparse_index_counts_as_filtered(
        self,
        mongo_client: MongoClient,
    ) -> None:
        # Mongo's second way of saying "only some documents", and the mechanism `skip_null`
        # asks for. Reading only `partialFilterExpression` would refuse a correct deployment.
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index([("supersedes_id", 1)], unique=True, sparse=True)

        await _validate(
            mongo_client,
            (db_name, collection),
            UniqueTogether(fields=("supersedes_id",), skip_null=True),
        )

    async def test_declaring_nothing_validates_nothing(
        self,
        mongo_client: MongoClient,
    ) -> None:
        await _validate(mongo_client, await _collection(mongo_client))
