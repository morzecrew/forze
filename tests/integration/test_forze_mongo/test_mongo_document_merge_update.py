"""Mongo stores an update merged into a mapping or nested model, not the patch."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.document_merge_update import (
    MergeCreate,
    MergeDoc,
    MergeRead,
    MergeUpdate,
    assert_update_matching_refuses_a_merge,
    assert_updates_merge,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_an_update_merges_into_a_stored_mapping_and_model(mongo_client: MongoClient) -> None:
    collection = f"merge_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db_name, collection), write=(db_name, collection))
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
    spec = DocumentSpec(
        name="merge",
        read=MergeRead,
        write=DocumentWriteTypes(domain=MergeDoc, create_cmd=MergeCreate, update_cmd=MergeUpdate),
    )

    await assert_updates_merge(ctx.document.command(spec), ctx.document.query(spec))
    await assert_update_matching_refuses_a_merge(
        ctx.document.command(spec), ctx.document.query(spec)
    )
