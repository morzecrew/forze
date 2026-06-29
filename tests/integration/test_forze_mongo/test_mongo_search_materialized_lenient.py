"""Integration tests: Mongo text search filters/sorts by a materialized field and
tolerates a lenient read field.

``SearchSpec.materialized`` makes a ``@computed_field`` a real document field, so Mongo
search can sort/filter by the derived value; ``lenient_read_fields`` lets a returned
field be absent and hydrate from its default.
"""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel, computed_field

from forze.application.contracts.querying import QueryFilterExpression
from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps, ExecutionContext
from forze_mongo.execution.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.configs import MongoSearchConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps


class MatDoc(BaseModel):
    id: UUID
    title: str
    qty: int
    unit_price: float
    nickname: str = "anon"  # returned, not stored (lenient)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total(self) -> float:
        return self.qty * self.unit_price


def _ctx(mongo_client: MongoClient, *, db_name: str, collection: str) -> ExecutionContext:
    return context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(read=(db_name, collection), engine="text")
                ),
            }
        )
    )


async def _seed(mongo_client: MongoClient) -> tuple[str, str]:
    db_name = (await mongo_client.db()).name
    collection = f"mat_search_{uuid4().hex[:10]}"
    coll = await mongo_client.collection(collection, db_name=db_name)
    await coll.create_index([("title", "text")])

    rows = [("widget alpha", 2, 5.0), ("widget beta", 3, 10.0), ("widget gamma", 1, 4.0)]
    await coll.insert_many(
        [
            {
                "_id": str(uuid4()),
                "id": str(uuid4()),
                "title": title,
                "qty": qty,
                "unit_price": price,
                "total": qty * price,  # the document side would write this column
            }
            for title, qty, price in rows
        ]
    )
    return db_name, collection


def _spec() -> SearchSpec[MatDoc]:
    return SearchSpec(
        name="mat_articles",
        model_type=MatDoc,
        fields=("title",),
        materialized={"total"},
        lenient_read_fields={"nickname"},
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_search_sorts_by_materialized(mongo_client: MongoClient) -> None:
    db_name, collection = await _seed(mongo_client)
    ctx = _ctx(mongo_client, db_name=db_name, collection=collection)
    adapter = ctx.search.query(_spec())

    page = await adapter.search_page("widget", sorts={"total": "desc"})

    assert [hit.total for hit in page.hits] == [30.0, 10.0, 4.0]
    # The lenient field is absent from the documents and hydrates from the default.
    assert all(hit.nickname == "anon" for hit in page.hits)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mongo_search_filters_by_materialized(mongo_client: MongoClient) -> None:
    db_name, collection = await _seed(mongo_client)
    ctx = _ctx(mongo_client, db_name=db_name, collection=collection)
    adapter = ctx.search.query(_spec())

    # Filter-only (empty text) — avoids Mongo's "$match with $text must be first" rule,
    # which is an engine constraint unrelated to the materialized column.
    flt: QueryFilterExpression = {"$values": {"total": {"$gte": 10.0}}}
    page = await adapter.search_page("", filters=flt)

    assert page.count == 2
    assert {hit.total for hit in page.hits} == {10.0, 30.0}
