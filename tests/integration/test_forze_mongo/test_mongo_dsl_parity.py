"""Cross-backend DSL parity: Mongo must reproduce the mock oracle.

Runs the shared query-DSL corpus against a real Mongo (testcontainers) and asserts
every supported case matches the same rows the in-memory mock produced. Mongo has
full AST-level capabilities, so every case runs — this is where a semantic divergence
(e.g. ``$all`` with an ordering predicate compiled via a min/max shortcut) surfaces as
a failing case instead of silently returning wrong rows.
"""

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
from forze_mongo.kernel.query.render import MONGO_QUERY_CAPABILITIES
from tests.support.execution_context import context_from_deps
from tests.support.query_dsl_corpus import (
    CombinedDocPort,
    CorpusCreate,
    CorpusDoc,
    CorpusRead,
    run_parity_cases,
)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dsl_parity_mongo(mongo_client: MongoClient) -> None:
    collection = f"dsl_corpus_{uuid4().hex[:8]}"
    db_name = (await mongo_client.db()).name

    spec = DocumentSpec(
        name="dsl_corpus_ns",
        read=CorpusRead,
        write=DocumentWriteTypes(domain=CorpusDoc, create_cmd=CorpusCreate),
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
    doc = CombinedDocPort(
        command=ctx.document.command(spec),
        query=ctx.document.query(spec),
    )

    await run_parity_cases(doc, MONGO_QUERY_CAPABILITIES, backend="mongo")
