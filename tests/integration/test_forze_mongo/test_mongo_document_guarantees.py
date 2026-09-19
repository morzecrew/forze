"""Integration: a declared storage guarantee validated against a real Mongo.

The Mongo half of the same rule Postgres follows — reconciliation at wiring says the store
*can* keep a uniqueness guarantee, and only the live collection says whether the deployment
created the index. A filtered guarantee needs a filtered index, and the distinction is the one
worth testing: a plain unique index accepted for "one *current* document per fact" would refuse
every superseded document the guarantee meant to allow.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from bson import Decimal128

from forze.application.contracts.guarantees import NonOverlapping, UniqueTogether
from forze.application.execution import Deps, LifecyclePlan
from forze.base.exceptions import CoreException
from forze_mongo.execution.deps import MongoClientDepKey
from forze_mongo.execution.document_indexes import (
    mongo_document_index_validation_lifecycle_step,
)
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.introspect import MongoIntrospector
from forze_mongo.kernel.validate_indexes import MongoDocumentIndexSpec
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

ONE_CURRENT = UniqueTogether(fields=("root_id",), where={"$values": {"is_current": True}})
ONE_EVER = UniqueTogether(fields=("root_id",))


def _mongosh_literal(rendered: str) -> dict[str, object]:
    """Parse the one-field object the refusal printed, the way ``mongosh`` would read it.

    Written out rather than reusing the renderer's own helpers: a parser that shared code with
    the thing under test would agree with it by construction, which is the opposite of what
    this leg is for.
    """

    inner = rendered.strip().removeprefix("{").removesuffix("}").strip()
    name, _, literal = inner.partition(":")
    literal = literal.strip()

    if found := re.fullmatch(r'(Long|Decimal128|ISODate)\("(.*)"\)', literal):
        kind, raw = found.groups()

        if kind == "Long":
            value: object = int(raw)

        elif kind == "Decimal128":
            value = Decimal128(raw)

        else:
            value = datetime.fromisoformat(raw)

    else:
        value = json.loads(literal)

    return {name.strip().strip('"'): value}


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

        with pytest.raises(CoreException, match="partialFilterExpression"):
            await _validate(mongo_client, (db_name, collection), ONE_CURRENT)

    async def test_a_plain_index_satisfies_an_unfiltered_guarantee(
        self,
        mongo_client: MongoClient,
    ) -> None:
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index([("root_id", 1)], unique=True)

        await _validate(mongo_client, (db_name, collection), ONE_EVER)

    async def test_a_sparse_index_does_not_satisfy_a_filtered_guarantee(
        self,
        mongo_client: MongoClient,
    ) -> None:
        # Sparse reads like "only some documents" and is not the same restriction: it skips a
        # document only when every indexed field is missing, and says nothing at all about the
        # guarantee's condition. Accepting it would pass a deployment where two current
        # documents for one fact are perfectly insertable.
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index([("root_id", 1)], unique=True, sparse=True)

        with pytest.raises(CoreException, match="partialFilterExpression"):
            await _validate(mongo_client, (db_name, collection), ONE_CURRENT)

    async def test_a_filter_over_other_fields_does_not_count(
        self,
        mongo_client: MongoClient,
    ) -> None:
        # The failure the field comparison exists for: right fields, wrong documents. Two
        # current-and-unverified documents for one fact fall outside this index entirely.
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index(
            [("root_id", 1)],
            unique=True,
            partialFilterExpression={"is_verified": True},
        )

        with pytest.raises(CoreException, match="partialFilterExpression"):
            await _validate(mongo_client, (db_name, collection), ONE_CURRENT)

    async def test_a_filter_over_the_right_field_and_the_wrong_value_does_not_count(
        self,
        mongo_client: MongoClient,
    ) -> None:
        # Read back from the server rather than asserted against a fixture, because what is
        # compared is the filter Mongo actually stored. Two current documents for one fact both
        # sit outside this index, so the guarantee would not be kept.
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index(
            [("root_id", 1)],
            unique=True,
            partialFilterExpression={"is_current": False},
        )

        with pytest.raises(CoreException, match="partialFilterExpression"):
            await _validate(mongo_client, (db_name, collection), ONE_CURRENT)

    async def test_a_numeric_one_does_not_serve_a_boolean_filter(
        self,
        mongo_client: MongoClient,
    ) -> None:
        # Read back from the server, because the claim is about what Mongo stores and how it
        # compares: a boolean and a number are different BSON types, so this index covers none
        # of the documents the guarantee selects and two current facts are insertable.
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index(
            [("root_id", 1)],
            unique=True,
            partialFilterExpression={"is_current": 1},
        )

        with pytest.raises(CoreException, match="partialFilterExpression"):
            await _validate(mongo_client, (db_name, collection), ONE_CURRENT)

    @pytest.mark.parametrize(
        ("value", "literal"),
        [
            (True, "true"),
            ("current", '"current"'),
            (7, "7"),
            (9007199254740993, 'Long("9007199254740993")'),
            (Decimal("9.99"), 'Decimal128("9.99")'),
            (date(2026, 9, 19), 'ISODate("2026-09-19T00:00:00+00:00")'),
            (datetime(2026, 9, 19, 12, 30, tzinfo=UTC), 'ISODate("2026-09-19T12:30:00+00:00")'),
            (
                # Finer than BSON keeps: the server truncates to milliseconds, so a declaration
                # carrying microseconds has to be compared and printed at what will be stored.
                datetime(2026, 9, 19, 12, 30, 0, 123567, tzinfo=UTC),
                'ISODate("2026-09-19T12:30:00.123000+00:00")',
            ),
            (datetime(2026, 9, 19, 12, 30), 'ISODate("2026-09-19T12:30:00+00:00")'),
            (
                UUID("00000000-0000-0000-0000-00000000002a"),
                '"00000000-0000-0000-0000-00000000002a"',
            ),
            (None, "null"),
        ],
    )
    async def test_the_printed_migration_creates_an_index_that_validates(
        self,
        mongo_client: MongoClient,
        value: object,
        literal: str,
    ) -> None:
        """Run what the refusal printed, then validate again.

        The refusal's whole value is that an operator can act on it, and the only proof of that
        is running the statement. Over every value type a filter admits rather than one of
        them: a boolean round-trips under almost any rendering, so a leg that only covered
        booleans passed while a large integer was being rounded to a different value and a
        `date` was creating an index the next startup refused.

        The statement is parsed out of the message rather than rebuilt here, so a message that
        drifts from the check fails this leg.
        """

        db_name, collection = await _collection(mongo_client)
        guarantee = UniqueTogether(fields=("root_id",), where={"$values": {"marker": value}})

        with pytest.raises(CoreException) as caught:
            await _validate(mongo_client, (db_name, collection), guarantee)

        # The summary wraps, so the statement is read out of the whole message.
        statement = " ".join(caught.value.summary.split())

        assert f"partialFilterExpression: {{marker: {literal}}}" in statement, statement

        found = re.search(r"partialFilterExpression: (\{.*?\})", statement)

        assert found is not None, statement

        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index(
            [("root_id", 1)],
            unique=True,
            partialFilterExpression=_mongosh_literal(found.group(1)),
        )

        await _validate(mongo_client, (db_name, collection), guarantee)

    async def test_a_reversed_compound_index_counts(
        self,
        mongo_client: MongoClient,
    ) -> None:
        # Uniqueness over a tuple does not depend on the order the index lists it in, and a
        # check that demanded the declared order would fail a correct migration at startup.
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index([("is_current", 1), ("root_id", 1)], unique=True)

        await _validate(
            mongo_client,
            (db_name, collection),
            UniqueTogether(fields=("root_id", "is_current")),
        )

    async def test_a_sparse_index_does_not_satisfy_an_unfiltered_guarantee(
        self,
        mongo_client: MongoClient,
    ) -> None:
        # The other direction: an unfiltered guarantee covers every document, and a sparse
        # index leaves out the ones missing the field — so the tuples it does not index are
        # unconstrained.
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index([("root_id", 1)], unique=True, sparse=True)

        with pytest.raises(CoreException, match="covering every document"):
            await _validate(mongo_client, (db_name, collection), ONE_EVER)

    async def test_a_member_no_store_maps_is_skipped_rather_than_crashing(
        self,
        mongo_client: MongoClient,
    ) -> None:
        # As on Postgres: the member arrives when a store maps it, and until then validation
        # must pass it by rather than read fields it does not have.
        relation = await _collection(mongo_client)
        step = mongo_document_index_validation_lifecycle_step(
            specs=[
                MongoDocumentIndexSpec(
                    name="fact",
                    write_relation=relation,
                    guarantees=(NonOverlapping(key=("root_id",), period=("a", "b")),),
                ),
            ],
        )
        ctx = context_from_deps(Deps.plain({MongoClientDepKey: mongo_client}))

        await LifecyclePlan.from_steps(step).freeze().startup(ctx)

    async def test_an_index_reports_its_restriction_and_its_sparseness_apart(
        self,
        mongo_client: MongoClient,
    ) -> None:
        # Read off the live server rather than a fixture, because the whole point is that these
        # are two different index options and only the server settles what it stored.
        db_name, collection = await _collection(mongo_client)
        coll = await mongo_client.collection(collection, db_name=db_name)
        await coll.create_index(
            [("root_id", 1)],
            unique=True,
            partialFilterExpression={"is_current": True},
            name="filtered",
        )
        await coll.create_index([("label", 1)], sparse=True, name="thin")

        introspector = MongoIntrospector(client=mongo_client)
        indexes = {
            index.name: index
            for index in await introspector.list_indexes(
                database=db_name,
                collection=collection,
            )
        }

        assert indexes["filtered"].partial_filter == {"is_current": True}
        assert indexes["filtered"].sparse is False
        assert indexes["thin"].partial_filter is None
        assert indexes["thin"].sparse is True

    async def test_declaring_nothing_validates_nothing(
        self,
        mongo_client: MongoClient,
    ) -> None:
        await _validate(mongo_client, await _collection(mongo_client))
