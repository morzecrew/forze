"""Mongo ``$having`` parity: a post-group ``$match`` matches the in-memory oracle."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps
from forze_mock.adapters import MockDocumentAdapter, MockState
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.aggregate_having import (
    AggCreate,
    AggDoc,
    AggRead,
    assert_aggregate_having_parity,
    seed_aggregate_corpus,
)
from tests.support.execution_context import context_from_deps


def _mock_oracle() -> MockDocumentAdapter[Any, Any, Any, Any]:
    spec = DocumentSpec(
        name="agg",
        read=AggRead,
        write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="agg",
        read_model=AggRead,
        domain_model=AggDoc,
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_aggregate_having_mongo(mongo_client: MongoClient) -> None:
    collection = f"agg_having_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name

    spec = DocumentSpec(
        name="agg",
        read=AggRead,
        write=DocumentWriteTypes(domain=AggDoc, create_cmd=AggCreate),
    )
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db_name, collection),
            write=(db_name, collection),
        )
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                MongoClientDepKey: mongo_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )

    await seed_aggregate_corpus(ctx.document.command(spec))

    oracle = _mock_oracle()
    await seed_aggregate_corpus(oracle)

    await assert_aggregate_having_parity(ctx.document.query(spec), oracle)
