"""Paired system invariants over Mongo: exact ``Decimal128`` sums and an order with no lines."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import DocumentCommandDepKey, DocumentQueryDepKey
from forze.application.execution import Deps
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps
from tests.support.paired_invariants import LINES, NOTES, ORDERS, assert_paired_laws_on

# ----------------------- #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_paired_laws_over_mongo(mongo_client: MongoClient) -> None:
    db_name = (await mongo_client.db()).name
    suffix = uuid4().hex[:8]

    def _document(collection: str) -> ConfigurableMongoDocument:
        return ConfigurableMongoDocument(
            config=MongoDocumentConfig(read=(db_name, collection), write=(db_name, collection))
        )

    documents = {
        spec.name: _document(f"inv_{spec.name}_{suffix}") for spec in (ORDERS, LINES, NOTES)
    }
    ctx = context_from_deps(
        Deps.plain({MongoClientDepKey: mongo_client}).merge(
            Deps.routed({DocumentQueryDepKey: documents, DocumentCommandDepKey: documents})
        )
    )

    await assert_paired_laws_on(ctx)
