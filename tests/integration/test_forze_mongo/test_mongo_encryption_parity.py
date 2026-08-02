"""The Mongo leg of the field-encryption battery.

Field encryption is wired per plane, and a helper wired for one plane is not enforcement on
the next — the sealed-sort refusal was a *wiring* gap when it was found, correct on the
mock and missing where it mattered. Mongo ran the round-trip tests but had never been held
to the whole observable set the Postgres leg is: the same refusals, the same searchable
hits, the same envelope at rest.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze_mongo.execution.deps.configs import MongoDocumentConfig
from forze_mongo.execution.deps.factories import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps
from tests.support.field_encryption_conformance import (
    FIELD_ENCRYPTION_BATTERY,
    Check,
    FieldEncryptionHarness,
    crypto_deps,
    spec,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _ctx(mongo_client: MongoClient, db: str, collection: str) -> ExecutionContext:
    configurable = ConfigurableMongoDocument(
        config=MongoDocumentConfig(read=(db, collection), write=(db, collection))
    )

    return context_from_deps(
        Deps.merge(
            crypto_deps(),
            Deps.plain(
                {
                    MongoClientDepKey: mongo_client,
                    DocumentQueryDepKey: configurable,
                    DocumentCommandDepKey: configurable,
                }
            ),
        )
    )


@pytest_asyncio.fixture
async def harness(mongo_client: MongoClient) -> FieldEncryptionHarness:
    database = await mongo_client.db()
    collection = f"people_parity_{uuid4().hex[:8]}"
    ctx = _ctx(mongo_client, database.name, collection)
    resolved = spec()

    return FieldEncryptionHarness(
        query=ctx.document.query(resolved),
        command=ctx.document.command(resolved),
        plain_query=ctx.document.query(spec(encrypted=False)),
        backend="mongo",
    )


@pytest.mark.conformance(plane="field_encryption", engine="mongo")
@pytest.mark.parametrize("check", FIELD_ENCRYPTION_BATTERY, ids=lambda check: check.__name__)
async def test_field_encryption_battery(check: Check, harness: FieldEncryptionHarness) -> None:
    await check(harness)
