"""Mongo text search against the shared search battery.

Like Postgres, Mongo searches the system of record: the corpus is inserted into the
collection the adapter reads, behind a compound text index.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.search import SearchQueryDepKey, SearchSpec
from forze.application.execution import Deps
from forze_mongo.execution.deps import ConfigurableMongoSearch
from forze_mongo.execution.deps.configs import MongoSearchConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps
from tests.support.search_conformance import (
    SEARCH_BATTERY,
    Check,
    SearchHarness,
    corpus_rows,
    searchable_fields,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Row(BaseModel):
    id: UUID
    title: str
    content: str
    category: str = ""


@pytest_asyncio.fixture
async def harness(mongo_client: MongoClient) -> SearchHarness:
    db_name = (await mongo_client.db()).name
    collection = f"search_conf_{uuid4().hex[:10]}"
    coll = await mongo_client.collection(collection, db_name=db_name)

    await coll.create_index([("title", "text"), ("content", "text")])
    await coll.insert_many(
        [{"_id": row["id"], **row} for row in corpus_rows(lambda: str(uuid4()))]
    )

    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                SearchQueryDepKey: ConfigurableMongoSearch(
                    config=MongoSearchConfig(read=(db_name, collection), engine="text")
                ),
            }
        )
    )

    spec = SearchSpec(name="rows", model_type=_Row, fields=searchable_fields())

    return SearchHarness(
        query=ctx.search.query(spec),
        backend="mongo_text",
        blank_query_matches_all=True,
    )


@pytest.mark.parametrize("check", SEARCH_BATTERY, ids=lambda check: check.__name__)
async def test_search_battery(check: Check, harness: SearchHarness) -> None:
    await check(harness)
