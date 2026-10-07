"""Mongo updates a field-encrypted document of every field type."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import CryptoDepsModule, Deps
from forze_mock import MockKeyManagement
from forze_mongo.execution.deps import ConfigurableMongoDocument, MongoDocumentConfig
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.encrypted_update import (
    ENCRYPTION,
    KEY,
    EncCreate,
    EncDoc,
    EncRead,
    EncUpdate,
    assert_encrypted_updates,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_a_sealed_field_of_any_type_updates(mongo_client: MongoClient) -> None:
    collection = f"enc_upd_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name
    doc = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db_name, collection), write=(db_name, collection))
    )
    ctx = context_from_deps(
        Deps.merge(
            CryptoDepsModule(kms=MockKeyManagement(), directory=KEY)(),
            Deps.plain(
                {
                    MongoClientDepKey: mongo_client,
                    DocumentQueryDepKey: doc,
                    DocumentCommandDepKey: doc,
                }
            ),
        )
    )
    spec = DocumentSpec(
        name="enc",
        read=EncRead,
        write=DocumentWriteTypes(domain=EncDoc, create_cmd=EncCreate, update_cmd=EncUpdate),
        encryption=ENCRYPTION,
    )

    async def raw(pk: UUID) -> dict[str, Any]:
        found = await (await mongo_client.db())[collection].find_one({"_id": str(pk)})
        assert found is not None
        return dict(found)

    await assert_encrypted_updates(ctx.document.command(spec), ctx.document.query(spec), raw)
