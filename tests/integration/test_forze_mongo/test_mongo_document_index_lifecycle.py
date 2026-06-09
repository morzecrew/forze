"""Integration tests for Mongo document index validation lifecycle."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.execution import Deps, LifecyclePlan
from forze_mongo.execution.deps import MongoClientDepKey
from forze_mongo.execution.document_indexes import (
    mongo_document_index_validation_lifecycle_step,
)
from forze_mongo.kernel.client import MongoClient
from forze_mongo.kernel.validate_indexes import MongoDocumentIndexSpec
from tests.support.execution_context import context_from_deps


@pytest.mark.integration
@pytest.mark.asyncio
async def test_document_index_validation_startup_runs(mongo_client: MongoClient) -> None:
    db_name = (await mongo_client.db()).name
    collection = f"idx_lifecycle_{uuid4().hex[:8]}"
    coll = await mongo_client.collection(collection, db_name=db_name)
    await coll.insert_one({"_id": "seed", "name": "x"})

    step = mongo_document_index_validation_lifecycle_step(
        specs=[
            MongoDocumentIndexSpec(
                name="docs",
                write_relation=(db_name, collection),
            ),
        ],
    )
    ctx = context_from_deps(Deps.plain({MongoClientDepKey: mongo_client}))
    plan = LifecyclePlan.from_steps(step)

    await plan.freeze().startup(ctx)
