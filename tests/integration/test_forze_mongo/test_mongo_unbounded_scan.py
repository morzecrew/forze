"""A read with no limit on Mongo returns every row once, ties in id order."""

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
from tests.support.unbounded_scan_parity import (
    ScanCreate,
    ScanDoc,
    ScanRead,
    run_unbounded_scan_parity,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_a_read_without_a_limit_orders_ties_by_id(mongo_client: MongoClient) -> None:
    collection = f"scan_{uuid4().hex[:8]}"
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
        name="scan",
        read=ScanRead,
        write=DocumentWriteTypes(domain=ScanDoc, create_cmd=ScanCreate),
    )

    await run_unbounded_scan_parity(ctx.document.command(spec), ctx.document.query(spec))
