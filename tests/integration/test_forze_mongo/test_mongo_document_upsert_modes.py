"""Mongo writes a mirrored and a ``skip_none`` upsert as the other stores do; with
``set_based`` it takes its usual path."""

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
from tests.support.upsert_modes import (
    ModeCreate,
    ModeDoc,
    ModeRead,
    ModeUpdate,
    assert_upsert_modes,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.mark.parametrize("set_based", [False, True])
async def test_a_mirror_and_skip_none_write_what_the_other_stores_write(
    mongo_client: MongoClient, set_based: bool
) -> None:
    collection = f"modes_{uuid4().hex[:8]}"
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
        name="modes",
        read=ModeRead,
        write=DocumentWriteTypes(domain=ModeDoc, create_cmd=ModeCreate, update_cmd=ModeUpdate),
    )

    await assert_upsert_modes(
        ctx.document.command(spec),
        ctx.document.query(spec),
        [uuid4() for _ in range(3)],
        set_based=set_based,
    )
