"""Mongo finds a resent unchanged instant unchanged against the aware BSON date it reads back."""

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
from tests.support.document_unchanged_instant import (
    InstantCreate,
    InstantDoc,
    InstantRead,
    InstantUpdate,
    assert_an_unchanged_instant_is_not_a_change,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_an_unchanged_instant_is_not_a_change(mongo_client: MongoClient) -> None:
    collection = f"instant_{uuid4().hex[:8]}"
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
        name="instant",
        read=InstantRead,
        write=DocumentWriteTypes(
            domain=InstantDoc, create_cmd=InstantCreate, update_cmd=InstantUpdate
        ),
    )

    await assert_an_unchanged_instant_is_not_a_change(
        ctx.document.command(spec), ctx.document.query(spec)
    )
