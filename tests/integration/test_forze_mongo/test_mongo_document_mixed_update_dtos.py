"""Mongo writes each patch of a mixed-type ``update_many`` whole."""

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
from tests.support.execution_context import context_from_deps
from tests.support.mixed_update_dtos import (
    MixedCreate,
    MixedDoc,
    MixedRead,
    MixedUpdate,
    assert_mixed_update_dtos,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_each_patch_of_a_mixed_type_update_many_is_written_whole(
    mongo_client: MongoClient,
) -> None:
    collection = f"mixed_{uuid4().hex[:8]}"
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
        name="mixed_dtos",
        read=MixedRead,
        write=DocumentWriteTypes(domain=MixedDoc, create_cmd=MixedCreate, update_cmd=MixedUpdate),
    )

    await assert_mixed_update_dtos(ctx.document.command(spec), ctx.document.query(spec))
